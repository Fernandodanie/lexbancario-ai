"""
ingest_normativa_paralelo.py

Fase 4 - Tareas Practicas de Sistemas Paralelos
--------------------------------------------------
Version modificada de scripts/ingest_normativa.py que paraleliza
UNICAMENTE la fase CPU-bound (extraccion de texto / OCR con Tesseract),
usando concurrent.futures.ThreadPoolExecutor.

La fase I/O-bound (llamadas a la API de embeddings de Gemini e insercion
en Supabase) se mantiene SECUENCIAL a proposito, respetando BATCH_SIZE
y las pausas de time.sleep(), para no exceder las cuotas de la API
(rate limiting / TPM-RPM).

Analisis del cuello de botella (para el informe):
- CPU-bound: extraer_texto_pdf() -> convert_from_path() + pytesseract.image_to_string()
  (lineas equivalentes 39-44 del script original). Consume nucleos del
  procesador durante el OCR de documentos escaneados.
- I/O-bound: generar_embeddings_con_reintento() -> ai_client.models.embed_content()
  + time.sleep(1.5) / time.sleep(40) en caso de rate limit (lineas 102-108
  y 117-118 del original). Aqui la CPU esta practicamente ociosa, solo
  se espera respuesta de red.
- En el script original, todo corre en un unico hilo y de forma
  secuencial (for archivo in archivos), por lo que el computo OCR de
  un archivo y la espera de red de otro nunca se solapan.
"""

import os
import re
import sys
import time
import pymupdf
import pytesseract
from pdf2image import convert_from_path
from dotenv import load_dotenv
from supabase import create_client, Client
from google import genai
from google.genai import types
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_API_KEY)

MODEL_EMBEDDING = "models/gemini-embedding-001"
BATCH_SIZE = 10  # Tamano seguro para evitar saturacion de TPM
MAX_WORKERS_OCR = 4  # Numero de PDFs procesados en paralelo (fase CPU-bound)


def extraer_texto_pdf(ruta_pdf):
    """Fase CPU-bound: texto nativo o, si no hay, OCR con Tesseract."""
    doc = pymupdf.open(ruta_pdf)
    texto_completo = []

    # Intento 1: Texto nativo digital
    for num_pagina, pagina in enumerate(doc):
        t = pagina.get_text().strip()
        if t:
            texto_completo.append((num_pagina + 1, t))

    # Intento 2: OCR si esta escaneado
    if not texto_completo:
        print(f"    [i] {os.path.basename(ruta_pdf)}: PDF escaneado detectado. Procesando OCR...")
        try:
            imagenes = convert_from_path(ruta_pdf)
            for idx, img in enumerate(imagenes):
                print(f"        -> {os.path.basename(ruta_pdf)}: OCR pagina {idx + 1}/{len(imagenes)}...")
                txt = pytesseract.image_to_string(img, lang="spa")
                if txt.strip():
                    texto_completo.append((idx + 1, txt))
        except Exception as e:
            print(f"    [X] Error durante OCR en {ruta_pdf}: {e}")

    return texto_completo


def parsear_articulos(paginas_texto, nombre_doc):
    if not paginas_texto:
        return []

    texto_unido = "\n".join([t[1] for t in paginas_texto]).strip()
    if not texto_unido:
        return []

    patron = r'(?i)(art[ií]culo\s+\d+[\.º°\-–\s]*[^\n]*)'
    fragmentos = re.split(patron, texto_unido)
    registros = []

    if len(fragmentos) <= 1:
        bloque_tam = 1200
        solapamiento = 200
        inicio = 0
        while inicio < len(texto_unido):
            fin = min(inicio + bloque_tam, len(texto_unido))
            chunk = texto_unido[inicio:fin].strip()
            if len(chunk) > 80:
                registros.append({
                    "documento_origen": nombre_doc,
                    "organismo": "ASFI / Banco Unión",
                    "tipo_norma": "Circular / Resolución",
                    "jerarquia": "RNSF",
                    "articulo_ref": f"Sección {len(registros) + 1}",
                    "contenido": chunk
                })
            inicio += (bloque_tam - solapamiento)
        return registros

    for i in range(1, len(fragmentos), 2):
        encabezado = fragmentos[i].strip()
        cuerpo = fragmentos[i + 1].strip() if (i + 1) < len(fragmentos) else ""
        contenido_total = f"{encabezado}\n{cuerpo}".strip()

        if len(contenido_total) > 40:
            registros.append({
                "documento_origen": nombre_doc,
                "organismo": "ASFI",
                "tipo_norma": "Circular / Resolución",
                "jerarquia": "RNSF",
                "articulo_ref": encabezado[:100],
                "contenido": contenido_total
            })

    return registros


def generar_embeddings_con_reintento(textos):
    """Fase I/O-bound: llamada de red a Gemini. Se mantiene secuencial
    a proposito para no exceder cuotas (TPM/RPM)."""
    intentos_rate_limit = 0
    while True:
        try:
            res = ai_client.models.embed_content(
                model=MODEL_EMBEDDING,
                contents=textos,
                config=types.EmbedContentConfig(output_dimensionality=768)
            )
            time.sleep(1.5)  # Pausa preventiva entre batches
            return [e.values for e in res.embeddings]
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                intentos_rate_limit += 1
                if intentos_rate_limit > 3:
                    print("\n[!] Se superó el límite diario de la API en este proyecto.")
                    print("    Genera una nueva API Key en otro proyecto de AI Studio para continuar.")
                    sys.exit(1)
                print("\n    [!] Pausa por límite de peticiones por minuto. Esperando 40 seg...")
                time.sleep(40)
            else:
                print(f"    [X] Error: {e}")
                raise e


def insertar_chunks(archivo, chunks):
    """Genera embeddings en batches e inserta en Supabase (secuencial, I/O-bound)."""
    total_chunks = len(chunks)
    print(f"    -> {archivo}: {total_chunks} fragmentos detectados")

    for i in range(0, total_chunks, BATCH_SIZE):
        lote = chunks[i:i + BATCH_SIZE]
        textos_lote = [item["contenido"] for item in lote]
        vectores = generar_embeddings_con_reintento(textos_lote)

        filas = []
        for item, vec in zip(lote, vectores):
            filas.append({
                "documento_origen": item["documento_origen"],
                "organismo": item["organismo"],
                "tipo_norma": item["tipo_norma"],
                "jerarquia": item["jerarquia"],
                "articulo_ref": item["articulo_ref"],
                "contenido": item["contenido"],
                "embedding": vec
            })

        supabase.table("normativa_bancaria").insert(filas).execute()
        print(f"    [✓] {archivo}: chunks {i + 1} a {min(i + BATCH_SIZE, total_chunks)} de {total_chunks} insertados.")


def procesar_directorio_paralelo():
    directorio = "documentos_pdf"
    if not os.path.exists(directorio):
        os.makedirs(directorio)

    archivos = [f for f in os.listdir(directorio) if f.lower().endswith(".pdf")]
    archivos = [f for f in archivos if f.lower() != "circulares.pdf"]

    # Descartar los que ya estan indexados en Supabase antes de gastar CPU en OCR
    pendientes = []
    for archivo in archivos:
        check = supabase.table("normativa_bancaria").select("id").eq("documento_origen", archivo).limit(1).execute()
        if check.data:
            print(f"[=] Omitiendo '{archivo}' (ya indexado en Supabase).")
        else:
            pendientes.append(archivo)

    if not pendientes:
        print("[!] No hay archivos PDF pendientes de procesar.")
        return

    print(f"[*] Total de archivos a procesar: {len(pendientes)} (hasta {MAX_WORKERS_OCR} en paralelo)")

    # --- Fase 1: extraccion de texto / OCR EN PARALELO (CPU-bound) ---
    resultados_extraccion = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_OCR) as executor:
        futuros = {
            executor.submit(extraer_texto_pdf, os.path.join(directorio, archivo)): archivo
            for archivo in pendientes
        }
        for futuro in as_completed(futuros):
            archivo = futuros[futuro]
            try:
                resultados_extraccion[archivo] = futuro.result()
                print(f"[✓] Extracción/OCR completada: {archivo}")
            except Exception as e:
                print(f"[X] Error procesando {archivo}: {e}")

    # --- Fase 2: parseo + embeddings + insercion, SECUENCIAL (I/O-bound) ---
    for archivo in pendientes:
        paginas = resultados_extraccion.get(archivo)
        if not paginas:
            print(f"    [X] {archivo}: documento sin texto.")
            continue

        chunks = parsear_articulos(paginas, archivo)
        if not chunks:
            continue

        insertar_chunks(archivo, chunks)

    print("\n[✓] Ingesta finalizada.")


if __name__ == "__main__":
    procesar_directorio_paralelo()