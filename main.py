import os
import time
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, model_validator
from dotenv import load_dotenv
from supabase import create_client, Client
from google import genai
from google.genai import types
import re
from ingesta import ejecutar_ingesta

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    raise RuntimeError("Faltan variables de entorno requeridas en el archivo .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_API_KEY)

MODEL_EMBEDDING = "models/gemini-embedding-001"
MODELOS_GENERACION = ["gemini-3.8-flash", "gemini-3.6-flash"]

app = FastAPI(
    title="API RAG Normativa Bancaria - ASFI / Banco Unión",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConsultaRequest(BaseModel):
    pregunta: str
    coleccion_id: str = Field(..., min_length=1, max_length=50, description="Namespace obligatorio de la colección a consultar")
    top_k: Optional[int] = 4
    match_threshold: Optional[float] = 0.35

class FuenteNormativa(BaseModel):
    documento: str
    articulo_ref: str
    similitud: float
    contenido: str

class ConsultaResponse(BaseModel):
    pregunta: str
    coleccion_id: str
    respuesta: str
    fuentes: List[FuenteNormativa]

def buscar_contexto(pregunta: str, coleccion_id: str, top_k: int, match_threshold: float):
    res_emb = ai_client.models.embed_content(
        model=MODEL_EMBEDDING,
        contents=[pregunta],
        config=types.EmbedContentConfig(output_dimensionality=768)
    )
    query_vector = res_emb.embeddings[0].values

    # Búsqueda restringida estrictamente a la colección (aislamiento multi-tenant)
    rpc_res = supabase.rpc("match_normativa_coleccion", {
        "query_embedding": query_vector,
        "filtro_coleccion": coleccion_id,
        "match_threshold": match_threshold,
        "match_count": top_k
    }).execute()

    return rpc_res.data or []

def generar_con_respaldo(prompt: str) -> str:
    """Genera respuesta con Gemini aplicando reintentos y fallback a modelo alternativo si hay 503."""
    ultimo_error = None
    for modelo in MODELOS_GENERACION:
        for intento in range(2):
            try:
                res = ai_client.models.generate_content(
                    model=modelo,
                    contents=prompt,
                    config=types.GenerateContentConfig(temperature=0.0)  # determinista
                )
                return res.text
            except Exception as e:
                ultimo_error = e
                err_str = str(e)
                if "503" in err_str or "UNAVAILABLE" in err_str:
                    time.sleep(2)
                    continue
                else:
                    break
    raise HTTPException(status_code=503, detail=f"Servicio LLM no disponible temporalmente: {ultimo_error}")

@app.get("/")
def estado():
    return {"status": "online", "mensaje": "API RAG de Normativa Bancaria activa"}

@app.post("/api/consultar", response_model=ConsultaResponse)
def consultar_normativa(req: ConsultaRequest):
    if not req.pregunta.strip():
        raise HTTPException(status_code=400, detail="La pregunta no puede estar vacía.")

    try:
        docs = buscar_contexto(req.pregunta, req.coleccion_id.strip(), req.top_k, req.match_threshold)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda vectorial: {e}")

    if not docs:
        # Regla de no-alucinación: sin antecedentes en la colección, se declara explícitamente
        return ConsultaResponse(
            pregunta=req.pregunta,
            coleccion_id=req.coleccion_id,
            respuesta=f"La colección '{req.coleccion_id}' no contiene antecedentes suficientes para responder esta consulta con certeza.",
            fuentes=[]
        )

    contexto_texto = ""
    fuentes_resp = []
    for d in docs:
        contexto_texto += f"\n--- DOCUMENTO: {d.get('documento_origen')} | REFERENCIA: {d.get('articulo_ref')} ---\n{d.get('contenido')}\n"
        fuentes_resp.append(FuenteNormativa(
            documento=d.get("documento_origen", ""),
            articulo_ref=d.get("articulo_ref", ""),
            similitud=round(d.get("similarity", 0) * 100, 2),
            contenido=d.get("contenido", "")
        ))

    prompt = f"""Eres un asesor especializado en la colección documental "{req.coleccion_id}".
Responde de forma rigurosa, clara y estructurada utilizando EXCLUSIVAMENTE el siguiente contexto.
Reglas:
1. No uses conocimiento externo ni inventes datos, artículos o cifras.
2. Si la respuesta no figura en el contexto, declara explícitamente que la colección no contiene antecedentes suficientes.
3. Cita siempre el documento y artículo o sección de respaldo.

CONTEXTO NORMATIVO:
{contexto_texto}

PREGUNTA:
{req.pregunta}

RESPUESTA:"""

    respuesta_texto = generar_con_respaldo(prompt)

    return ConsultaResponse(
        pregunta=req.pregunta,
        coleccion_id=req.coleccion_id,
        respuesta=respuesta_texto,
        fuentes=fuentes_resp
    )


# =====================================================================
# Ingesta web concurrente (Requerimiento 2)
# =====================================================================
PATRON_COLECCION = re.compile(r"^[A-Za-z0-9_\-]{1,50}$")


class IngestaWebRequest(BaseModel):
    coleccion_id: str = Field(..., pattern=r"^[A-Za-z0-9_\-]{1,50}$", description="Namespace de destino")
    urls: List[str] = Field(..., min_length=1, max_length=200)
    chunk_size: int = Field(1000, ge=200, le=4000)
    chunk_overlap: int = Field(200, ge=0, le=1000)
    concurrency_workers: int = Field(4, ge=1, le=16)
    reemplazar: bool = Field(True, description="Si la URL ya existe en la colección, se reemplaza (idempotencia)")

    @model_validator(mode="after")
    def validar_overlap(self):
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError("chunk_overlap debe ser menor que chunk_size")
        return self


def embed_lote(textos: List[str]):
    """Embeddings de un lote (llamada bloqueante; el pipeline la ejecuta en un hilo)."""
    res = ai_client.models.embed_content(
        model=MODEL_EMBEDDING,
        contents=textos,
        config=types.EmbedContentConfig(output_dimensionality=768)
    )
    return [e.values for e in res.embeddings]


@app.post("/api/ingestar-web")
async def ingestar_web(req: IngestaWebRequest):
    """Descarga, limpia, trocea, vectoriza e indexa URLs en paralelo bajo una colección."""
    return await ejecutar_ingesta(req, embed_lote, supabase)


@app.get("/api/coleccion/{coleccion_id}")
def estado_coleccion(coleccion_id: str):
    """Cantidad de chunks almacenados en una colección."""
    if not PATRON_COLECCION.match(coleccion_id):
        raise HTTPException(status_code=422, detail="coleccion_id inválido")
    r = supabase.table("normativa_bancaria").select("id", count="exact").eq("coleccion_id", coleccion_id).limit(1).execute()
    return {"coleccion_id": coleccion_id, "chunks": r.count or 0}


@app.delete("/api/coleccion/{coleccion_id}")
def borrar_coleccion(coleccion_id: str):
    """Elimina todos los chunks de una colección (útil entre corridas del benchmark)."""
    if not PATRON_COLECCION.match(coleccion_id):
        raise HTTPException(status_code=422, detail="coleccion_id inválido")
    supabase.table("normativa_bancaria").delete().eq("coleccion_id", coleccion_id).execute()
    return {"coleccion_id": coleccion_id, "eliminada": True}
