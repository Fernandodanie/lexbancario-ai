-- =====================================================================
-- PASO 0 - Esquema base (solo si el proyecto Supabase es NUEVO / vacio)
-- Ejecutar en: Supabase > SQL Editor, ANTES de 01_aislamiento_coleccion.sql
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS normativa_bancaria (
    id               BIGSERIAL PRIMARY KEY,
    documento_origen TEXT,
    organismo        TEXT,
    tipo_norma       TEXT,
    jerarquia        TEXT,
    articulo_ref     TEXT,
    contenido        TEXT NOT NULL,
    embedding        vector(768),
    created_at       TIMESTAMPTZ DEFAULT now()
);

-- Funcion original (sin filtro de coleccion), usada por el flujo actual
CREATE OR REPLACE FUNCTION match_normativa(
    query_embedding vector(768),
    match_threshold FLOAT,
    match_count     INT
)
RETURNS TABLE (
    id BIGINT, documento_origen TEXT, articulo_ref TEXT,
    contenido TEXT, similarity FLOAT
)
LANGUAGE sql STABLE
AS $$
    SELECT n.id, n.documento_origen, n.articulo_ref, n.contenido,
           (1 - (n.embedding <=> query_embedding))::FLOAT AS similarity
    FROM normativa_bancaria n
    WHERE 1 - (n.embedding <=> query_embedding) > match_threshold
    ORDER BY n.embedding <=> query_embedding
    LIMIT match_count;
$$;
