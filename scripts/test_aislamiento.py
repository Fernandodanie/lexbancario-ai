"""
PASO 1 - Prueba de aislamiento vectorial (cero contaminacion cruzada).

Inserta temporalmente textos de dos colecciones (bancaria vs tributaria),
consulta cada coleccion con preguntas de AMBOS dominios y verifica que
jamas se devuelva un fragmento de la otra coleccion.

Uso:  python scripts/test_aislamiento.py
"""
import os
import sys
from dotenv import load_dotenv
from supabase import create_client
from google import genai
from google.genai import types

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
ai = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
MODEL = "models/gemini-embedding-001"

COL_BANCA, COL_TRIB = "test_bancaria", "test_tributaria"

DOCS = {
    COL_BANCA: [
        "El encaje legal es el porcentaje de depositos que las entidades financieras deben mantener en el Banco Central.",
        "La ASFI regula la suficiencia patrimonial y el coeficiente de adecuacion patrimonial de los bancos.",
        "Las entidades de intermediacion financiera deben constituir previsiones por cartera incobrable.",
    ],
    COL_TRIB: [
        "El Impuesto al Valor Agregado (IVA) grava las ventas de bienes muebles con una alicuota del 13%.",
        "El Impuesto sobre las Utilidades de las Empresas (IUE) se calcula sobre la utilidad neta con una alicuota del 25%.",
        "Los contribuyentes deben emitir facturas y declarar el RC-IVA ante el Servicio de Impuestos Nacionales.",
    ],
}

PREGUNTAS = [
    "Que porcentaje de encaje legal deben mantener los bancos?",
    "Cual es la alicuota del IVA sobre las ventas?",
    "Como se calcula el impuesto sobre las utilidades?",
    "Que es la suficiencia patrimonial de una entidad financiera?",
]

def embed(textos):
    r = ai.models.embed_content(
        model=MODEL, contents=textos,
        config=types.EmbedContentConfig(output_dimensionality=768))
    return [e.values for e in r.embeddings]

def limpiar():
    for c in (COL_BANCA, COL_TRIB):
        supabase.table("normativa_bancaria").delete().eq("coleccion_id", c).execute()

def main():
    limpiar()
    print("[*] Insertando datos de prueba en dos colecciones...")
    for col, textos in DOCS.items():
        vecs = embed(textos)
        filas = [{
            "coleccion_id": col,
            "documento_origen": f"{col}.txt",
            "organismo": "TEST", "tipo_norma": "TEST", "jerarquia": "TEST",
            "articulo_ref": f"Test {i+1}",
            "contenido": t, "embedding": v,
        } for i, (t, v) in enumerate(zip(textos, vecs))]
        supabase.table("normativa_bancaria").insert(filas).execute()
        print(f"    [OK] {col}: {len(filas)} chunks")

    contaminaciones = 0
    print("\n[*] Consultas cruzadas (threshold bajo para forzar la mezcla si existiera)")
    for col in (COL_BANCA, COL_TRIB):
        print(f"\n=== Consultando SOLO coleccion '{col}' ===")
        for q in PREGUNTAS:
            qv = embed([q])[0]
            res = supabase.rpc("match_normativa_coleccion", {
                "query_embedding": qv,
                "filtro_coleccion": col,
                "match_threshold": 0.0,
                "match_count": 5,
            }).execute().data or []
            ajenos = [r for r in res if r["coleccion_id"] != col]
            contaminaciones += len(ajenos)
            print(f"  P: {q}")
            print(f"     -> {len(res)} resultados | de otra coleccion: {len(ajenos)}")
            for r in res:
                print(f"        [{r['coleccion_id']}] sim={r['similarity']:.3f} | {r['contenido'][:60]}...")

    limpiar()
    print("\n" + "=" * 60)
    if contaminaciones == 0:
        print("[OK] CERO CONTAMINACION CRUZADA: ningun resultado fuera de su coleccion")
    else:
        print(f"[FALLO] {contaminaciones} resultados contaminados")
    print("=" * 60)
    sys.exit(0 if contaminaciones == 0 else 1)

if __name__ == "__main__":
    main()