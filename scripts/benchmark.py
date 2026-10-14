"""
benchmark.py - Pasos 4 y 5: mide T_p, Speedup, Eficiencia y desglose por etapa.

Uso (con la API levantada en otra terminal):
    python scripts/benchmark.py
    python scripts/benchmark.py --workers 1 2 4 8 --pausa 90

Para cada p: borra la coleccion, ingesta las URLs con p workers y registra
tiempos. Salidas en evidencias/: benchmark_resultados.json/.csv y graficos .png
"""
import argparse, csv, json, os, time
import requests

ap = argparse.ArgumentParser()
ap.add_argument("--api", default="http://127.0.0.1:8000")
ap.add_argument("--urls", default="scripts/urls_benchmark.txt")
ap.add_argument("--coleccion", default="bench_tributaria")
ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
ap.add_argument("--chunk-size", type=int, default=1000)
ap.add_argument("--chunk-overlap", type=int, default=200)
ap.add_argument("--pausa", type=int, default=60, help="segundos entre corridas (recupera la cuota de la API)")
a = ap.parse_args()

urls = [l.strip() for l in open(a.urls, encoding="utf-8") if l.strip() and not l.startswith("#")]
os.makedirs("evidencias", exist_ok=True)
print(f"[*] {len(urls)} URLs | workers {a.workers} | coleccion '{a.coleccion}' | pausa {a.pausa}s")

corridas = []
for i, p in enumerate(a.workers):
    if i > 0:
        print(f"    ...pausa de {a.pausa}s para recuperar cuota")
        time.sleep(a.pausa)
    requests.delete(f"{a.api}/api/coleccion/{a.coleccion}", timeout=60).raise_for_status()
    print(f"[>] Corrida con p={p} workers")
    t0 = time.perf_counter()
    r = requests.post(f"{a.api}/api/ingestar-web", timeout=3600, json={
        "coleccion_id": a.coleccion, "urls": urls, "chunk_size": a.chunk_size,
        "chunk_overlap": a.chunk_overlap, "concurrency_workers": p, "reemplazar": True})
    r.raise_for_status()
    d = r.json()
    d["tiempo_cliente_s"] = round(time.perf_counter() - t0, 3)
    corridas.append(d)
    print(f"    T={d['tiempo_total_s']}s | ok={d['urls_ok']}/{d['urls_total']} | chunks={d['chunks_insertados']} | 429={d['rate_limit']['eventos_429']}")

# ---- metricas
base = next((c for c in corridas if c["workers"] == 1), corridas[0])
T1 = base["tiempo_total_s"]
filas = []
for c in corridas:
    p, Tp = c["workers"], c["tiempo_total_s"]
    Sp = T1 / Tp
    Ep = Sp / p
    f = (1 / Sp - 1 / p) / (1 - 1 / p) if p > 1 and Sp > 0 else None  # fraccion serial (Amdahl)
    dg = c["desglose"]
    filas.append({"workers": p, "T_p_s": Tp, "speedup": round(Sp, 3), "eficiencia": round(Ep, 3),
                  "fraccion_serial_amdahl": round(f, 3) if f is not None else "",
                  "speedup_max_amdahl": round(1 / f, 2) if f and f > 0 else "",
                  "urls_ok": c["urls_ok"], "chunks": c["chunks_insertados"],
                  "eventos_429": c["rate_limit"]["eventos_429"], "espera_429_s": c["rate_limit"]["espera_total_s"],
                  "pct_red": dg["pct_red"], "pct_cpu": dg["pct_cpu"],
                  "pct_embeddings": dg["pct_embeddings"], "pct_insercion": dg["pct_insercion"]})

print("\n p | T_p (s) | Speedup | Eficiencia | f serial | 429 | %red  %cpu  %emb  %ins")
for x in filas:
    print(f"{x['workers']:>2} | {x['T_p_s']:>7} | {x['speedup']:>7} | {x['eficiencia']:>10} | {str(x['fraccion_serial_amdahl']):>8} | "
          f"{x['eventos_429']:>3} | {x['pct_red']:>5} {x['pct_cpu']:>5} {x['pct_embeddings']:>5} {x['pct_insercion']:>5}")

json.dump({"urls": urls, "corridas": corridas, "metricas": filas}, open("evidencias/benchmark_resultados.json", "w", encoding="utf-8"), indent=2, ensure_ascii=False)
with open("evidencias/benchmark_resultados.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.DictWriter(fh, fieldnames=list(filas[0].keys())); w.writeheader(); w.writerows(filas)

# ---- graficos
try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    ps = [x["workers"] for x in filas]
    plt.figure(figsize=(6, 4)); plt.plot(ps, [x["speedup"] for x in filas], "o-", label="Speedup medido")
    plt.plot(ps, ps, "--", label="Ideal (S=p)"); plt.xlabel("Workers (p)"); plt.ylabel("Speedup S_p")
    plt.title("Aceleracion"); plt.legend(); plt.grid(alpha=.3); plt.tight_layout(); plt.savefig("evidencias/grafico_speedup.png", dpi=150); plt.close()
    plt.figure(figsize=(6, 4)); plt.plot(ps, [x["eficiencia"] for x in filas], "o-", color="tab:green")
    plt.ylim(0, 1.1); plt.xlabel("Workers (p)"); plt.ylabel("Eficiencia E_p"); plt.title("Eficiencia")
    plt.grid(alpha=.3); plt.tight_layout(); plt.savefig("evidencias/grafico_eficiencia.png", dpi=150); plt.close()
    plt.figure(figsize=(7, 4)); fondo = [0] * len(ps)
    for clave, nombre in [("pct_red", "Red/Scraping"), ("pct_cpu", "Chunking/CPU"), ("pct_embeddings", "API Embeddings"), ("pct_insercion", "Insercion Supabase")]:
        v = [x[clave] for x in filas]; plt.bar([str(p) for p in ps], v, bottom=fondo, label=nombre); fondo = [f + y for f, y in zip(fondo, v)]
    plt.xlabel("Workers (p)"); plt.ylabel("% del tiempo de etapas"); plt.title("Desglose por etapa"); plt.legend(fontsize=8)
    plt.tight_layout(); plt.savefig("evidencias/grafico_desglose.png", dpi=150); plt.close()
    print("\n[OK] Graficos guardados en evidencias/")
except ImportError:
    print("\n[!] Instala matplotlib para generar graficos: pip install matplotlib")
print("[OK] Resultados en evidencias/benchmark_resultados.json y .csv")
