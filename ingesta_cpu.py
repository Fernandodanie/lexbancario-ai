"""
ingesta_cpu.py - Funciones CPU-bound del pipeline de ingesta.

Se mantienen en un modulo aparte, SIN dependencias de red ni clientes
(Supabase/Gemini), porque se ejecutan en procesos hijos (ProcessPoolExecutor)
y deben ser livianas de importar y "picklables".
"""
import re
from bs4 import BeautifulSoup

ETIQUETAS_RUIDO = ["script", "style", "noscript", "nav", "footer", "header",
                   "aside", "form", "svg", "iframe", "table", "sup"]
SELECTORES_RUIDO = [".mw-editsection", ".navbox", ".infobox", ".reference",
                    ".mw-references-wrap", ".thumb", ".hatnote", ".metadata"]


def limpiar_html(html: str):
    """Devuelve (titulo, texto_limpio) a partir de HTML crudo."""
    soup = BeautifulSoup(html, "html.parser")
    titulo = soup.title.get_text(strip=True) if soup.title else ""
    for tag in soup(ETIQUETAS_RUIDO):
        if not tag.decomposed:
            tag.decompose()
    for selector in SELECTORES_RUIDO:
        for el in soup.select(selector):
            if not el.decomposed:
                el.decompose()
    principal = soup.find("main") or soup.find("article") or soup.body or soup
    texto = principal.get_text(separator="\n")
    texto = re.sub(r"[ \t\r\f\v]+", " ", texto)
    texto = re.sub(r"\n\s*\n+", "\n\n", texto).strip()
    return titulo, texto


def trocear(texto: str, chunk_size: int, chunk_overlap: int):
    """Ventana deslizante por caracteres con corte suave en espacio/salto."""
    if chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap debe ser menor que chunk_size")
    chunks, inicio, n = [], 0, len(texto)
    paso_min = chunk_size - chunk_overlap
    while inicio < n:
        fin = min(inicio + chunk_size, n)
        if fin < n:  # corte suave: buscar un separador en el ultimo 20% de la ventana
            ventana_ini = inicio + int(chunk_size * 0.8)
            corte = max(texto.rfind("\n", ventana_ini, fin), texto.rfind(" ", ventana_ini, fin))
            if corte > inicio:
                fin = corte
        chunk = texto[inicio:fin].strip()
        if len(chunk) > 80:
            chunks.append(chunk)
        if fin >= n:
            break
        inicio = max(fin - chunk_overlap, inicio + paso_min)
    return chunks


def procesar_html(html: str, chunk_size: int, chunk_overlap: int):
    """Tarea completa CPU-bound: limpieza + chunking. Devuelve (titulo, chunks)."""
    titulo, texto = limpiar_html(html)
    return titulo, trocear(texto, chunk_size, chunk_overlap)