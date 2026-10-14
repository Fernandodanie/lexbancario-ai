"""
ingesta.py - Pipeline concurrente de scraping + embeddings + indexacion.

Arquitectura (por cada URL, hasta `workers` URLs en paralelo):

  [asyncio + httpx]  descarga HTTP            (I/O-bound, red)
        |
  [ProcessPoolExecutor] limpieza HTML + chunking   (CPU-bound)
        |
  [asyncio.to_thread]  embeddings por lotes con backoff exponencial ante 429
        |                (I/O-bound, API externa; pausa GLOBAL no bloqueante)
  [asyncio.to_thread]  insercion masiva en Supabase por lotes (I/O-bound, BD)

Sincronizacion / ausencia de race conditions:
  * Todo el estado compartido (pausa por rate limit, contadores) se modifica
    SOLO desde el hilo del event loop (asyncio es cooperativo: no hay
    interleaving entre `await`s), por lo que no se necesitan locks de hilos.
  * Cada URL produce su propio objeto ResultadoURL; la agregacion se hace al
    final, una vez terminadas todas las tareas.
  * `workers` controla la concurrencia mediante asyncio.Semaphore y el tamano
    del ProcessPoolExecutor. workers=1 equivale al caso secuencial (T_1).
"""
import asyncio
import ipaddress
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from urllib.parse import urlparse

import httpx

from ingesta_cpu import procesar_html

USER_AGENT = "LexBancarioBot/1.0 (practica universitaria UAJMS; Sistemas Paralelos y Distribuidos)"
EMBED_BATCH = int(os.getenv("EMBED_BATCH", "20"))      # textos por llamada a embeddings
INSERT_BATCH = int(os.getenv("INSERT_BATCH", "50"))    # filas por insercion en Supabase
MAX_REINTENTOS_429 = int(os.getenv("MAX_REINTENTOS_429", "6"))
BACKOFF_BASE_S = float(os.getenv("BACKOFF_BASE_S", "2"))
BACKOFF_MAX_S = float(os.getenv("BACKOFF_MAX_S", "60"))
TIMEOUT_DESCARGA_S = float(os.getenv("TIMEOUT_DESCARGA_S", "20"))
REINTENTOS_DESCARGA = 2
MAX_BYTES = 5 * 1024 * 1024  # 5 MB por pagina


# ----------------------------------------------------------------- utilidades
def url_permitida(url: str) -> bool:
    """http/https y bloqueo basico de hosts locales/privados (anti-SSRF)."""
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    if os.getenv("PERMITIR_LOCAL") == "1":  # solo para pruebas locales
        return True
    host = p.hostname.lower()
    if host == "localhost" or host.endswith(".local"):
        return False
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return False
    except ValueError:
        pass  # es un nombre de dominio
    return True


def es_rate_limit(exc: Exception) -> bool:
    s = str(exc)
    return "429" in s or "RESOURCE_EXHAUSTED" in s or "rate limit" in s.lower()


@dataclass
class ResultadoURL:
    url: str
    estado: str = "pendiente"          # ok | error
    motivo: str = ""
    chunks: int = 0
    t_red: float = 0.0
    t_cpu: float = 0.0
    t_embed: float = 0.0
    t_insert: float = 0.0
    reintentos_429: int = 0


@dataclass
class EstadoGlobal:
    """Estado compartido; solo se toca desde el event loop."""
    pausa_hasta: float = 0.0           # instante (monotonic) hasta el que se pausa la API
    total_429: int = 0
    espera_429_s: float = 0.0


# ------------------------------------------------------------------- etapas
async def descargar(cliente: httpx.AsyncClient, url: str) -> str:
    ultimo = None
    for intento in range(REINTENTOS_DESCARGA + 1):
        try:
            r = await cliente.get(url)
            r.raise_for_status()
            tipo = r.headers.get("content-type", "")
            if "html" not in tipo and "text" not in tipo:
                raise ValueError(f"contenido no HTML ({tipo or 'desconocido'})")
            if len(r.content) > MAX_BYTES:
                raise ValueError("pagina demasiado grande")
            return r.text
        except (httpx.TimeoutException, httpx.TransportError) as e:
            ultimo = e
            await asyncio.sleep(0.5 * (2 ** intento))  # reintento con backoff
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (429, 500, 502, 503, 504) and intento < REINTENTOS_DESCARGA:
                ultimo = e
                await asyncio.sleep(1.0 * (2 ** intento))
                continue
            raise
    raise ultimo


async def embeber_lote(textos, embed_fn, estado: EstadoGlobal, res: ResultadoURL):
    """Embeddings de un lote con backoff exponencial + jitter ante 429.
    Las pausas usan asyncio.sleep (no bloquean el loop) y son GLOBALES:
    si un worker recibe 429, todos esperan antes de volver a llamar a la API."""
    for intento in range(MAX_REINTENTOS_429 + 1):
        espera_global = estado.pausa_hasta - time.monotonic()
        if espera_global > 0:
            await asyncio.sleep(espera_global)
        try:
            return await asyncio.to_thread(embed_fn, textos)
        except Exception as e:
            if not es_rate_limit(e) or intento == MAX_REINTENTOS_429:
                raise
            espera = min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** intento)) + random.uniform(0, 1)
            estado.total_429 += 1
            estado.espera_429_s += espera
            res.reintentos_429 += 1
            estado.pausa_hasta = max(estado.pausa_hasta, time.monotonic() + espera)
            print(f"    [429] {res.url[:60]} -> pausa global {espera:.1f}s (intento {intento + 1})")


def _insertar(supabase, filas):
    supabase.table("normativa_bancaria").insert(filas).execute()


def _borrar_documento(supabase, coleccion_id, url):
    supabase.table("normativa_bancaria").delete() \
        .eq("coleccion_id", coleccion_id).eq("documento_origen", url).execute()


async def procesar_url(url, req, cliente, pool, embed_fn, supabase, estado) -> ResultadoURL:
    res = ResultadoURL(url=url)
    loop = asyncio.get_running_loop()
    try:
        # 1) Red
        t0 = time.perf_counter()
        html = await descargar(cliente, url)
        res.t_red = time.perf_counter() - t0

        # 2) CPU (proceso aparte -> sin GIL)
        t0 = time.perf_counter()
        titulo, chunks = await loop.run_in_executor(
            pool, procesar_html, html, req.chunk_size, req.chunk_overlap)
        res.t_cpu = time.perf_counter() - t0
        if not chunks:
            raise ValueError("sin texto util tras la limpieza")

        # 3) Embeddings por lotes
        t0 = time.perf_counter()
        vectores = []
        for i in range(0, len(chunks), EMBED_BATCH):
            vectores.extend(await embeber_lote(chunks[i:i + EMBED_BATCH], embed_fn, estado, res))
        res.t_embed = time.perf_counter() - t0

        # 4) Insercion masiva (idempotente: reemplaza lo previo de esa URL en la coleccion)
        t0 = time.perf_counter()
        if req.reemplazar:
            await asyncio.to_thread(_borrar_documento, supabase, req.coleccion_id, url)
        dominio = urlparse(url).netloc
        filas = [{
            "coleccion_id": req.coleccion_id,
            "documento_origen": url,
            "organismo": dominio,
            "tipo_norma": "Web",
            "jerarquia": "Web",
            "articulo_ref": f"{(titulo or dominio)[:80]} - fragmento {n + 1}",
            "contenido": c,
            "embedding": v,
        } for n, (c, v) in enumerate(zip(chunks, vectores))]
        for i in range(0, len(filas), INSERT_BATCH):
            await asyncio.to_thread(_insertar, supabase, filas[i:i + INSERT_BATCH])
        res.t_insert = time.perf_counter() - t0

        res.chunks, res.estado = len(filas), "ok"
    except Exception as e:  # una URL caida/lenta NO aborta el resto del lote
        res.estado = "error"
        res.motivo = f"{type(e).__name__}: {str(e)[:200]}".replace("\n", " ")
    return res


# ----------------------------------------------------------------- orquestador
async def ejecutar_ingesta(req, embed_fn, supabase) -> dict:
    """req: objeto con coleccion_id, urls, chunk_size, chunk_overlap,
    concurrency_workers, reemplazar."""
    workers = req.concurrency_workers
    estado = EstadoGlobal()
    sem = asyncio.Semaphore(workers)
    t_inicio = time.perf_counter()

    async def tarea(url, cliente, pool):
        if not url_permitida(url):
            return ResultadoURL(url=url, estado="error", motivo="URL no permitida (esquema o host)")
        async with sem:  # maximo `workers` URLs en vuelo
            return await procesar_url(url, req, cliente, pool, embed_fn, supabase, estado)

    limites = httpx.Limits(max_connections=max(workers * 2, 4))
    cabeceras = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    with ProcessPoolExecutor(max_workers=workers) as pool:
        async with httpx.AsyncClient(timeout=TIMEOUT_DESCARGA_S, follow_redirects=True,
                                     headers=cabeceras, limits=limites) as cliente:
            resultados = await asyncio.gather(*(tarea(u, cliente, pool) for u in req.urls))

    t_total = time.perf_counter() - t_inicio
    suma = {k: sum(getattr(r, k) for r in resultados) for k in ("t_red", "t_cpu", "t_embed", "t_insert")}
    t_etapas = sum(suma.values()) or 1e-9
    ok = [r for r in resultados if r.estado == "ok"]
    return {
        "coleccion_id": req.coleccion_id,
        "workers": workers,
        "urls_total": len(resultados),
        "urls_ok": len(ok),
        "urls_error": len(resultados) - len(ok),
        "chunks_insertados": sum(r.chunks for r in ok),
        "tiempo_total_s": round(t_total, 3),
        "desglose": {
            "red_s": round(suma["t_red"], 3),
            "cpu_s": round(suma["t_cpu"], 3),
            "embeddings_s": round(suma["t_embed"], 3),
            "insercion_s": round(suma["t_insert"], 3),
            "pct_red": round(100 * suma["t_red"] / t_etapas, 1),
            "pct_cpu": round(100 * suma["t_cpu"] / t_etapas, 1),
            "pct_embeddings": round(100 * suma["t_embed"] / t_etapas, 1),
            "pct_insercion": round(100 * suma["t_insert"] / t_etapas, 1),
        },
        "rate_limit": {"eventos_429": estado.total_429, "espera_total_s": round(estado.espera_429_s, 1)},
        "detalle": [
            {"url": r.url, "estado": r.estado, "motivo": r.motivo, "chunks": r.chunks,
             "t_red_s": round(r.t_red, 3), "t_cpu_s": round(r.t_cpu, 3),
             "t_embed_s": round(r.t_embed, 3), "t_insert_s": round(r.t_insert, 3),
             "reintentos_429": r.reintentos_429} for r in resultados
        ],
    }
