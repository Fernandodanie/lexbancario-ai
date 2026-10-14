-- =====================================================================
-- PASO 1 - Aislamiento vectorial multi-tenant (coleccion_id)
-- Ejecutar en: Supabase > SQL Editor
-- =====================================================================

-- 1) DDL: nueva columna. Las filas ya existentes (normativa ASFI) quedan
--    en la coleccion 'normativa_bancaria' para no perder lo ya indexado.
ALTER TABLE normativa_bancaria
    ADD COLUMN IF NOT EXISTS coleccion_id VARCHAR(50) NOT NULL DEFAULT 'normativa_bancaria';

-- 2) Indice B-Tree sobre coleccion_id (acelera el filtro por namespace)
CREATE INDEX IF NOT EXISTS idx_normativa_coleccion_id
    ON normativa_bancaria USING btree (coleccion_id);

-- 3) Funcion RPC: distancia coseno (<=>) SOLO sobre la coleccion pedida
DROP FUNCTION IF EXISTS match_normativa_coleccion(vector, varchar, float, int);

CREATE OR REPLACE FUNCTION match_normativa_coleccion(
    query_embedding  vector(768),
    filtro_coleccion VARCHAR(50),
    match_threshold  FLOAT DEFAULT 0.35,
    match_count      INT   DEFAULT 4
)
RETURNS TABLE (
    id               BIGINT,
    coleccion_id     VARCHAR(50),
    documento_origen TEXT,
    articulo_ref     TEXT,
    contenido        TEXT,
    similarity       FLOAT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        n.id::BIGINT,
        n.coleccion_id,
        n.documento_origen::TEXT,
        n.articulo_ref::TEXT,
        n.contenido::TEXT,
        (1 - (n.embedding <=> query_embedding))::FLOAT AS similarity
    FROM normativa_bancaria n
    WHERE n.coleccion_id = filtro_coleccion              -- aislamiento duro
      AND (1 - (n.embedding <=> query_embedding)) > match_threshold
    ORDER BY n.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

-- =====================================================================
-- VERIFICACION (hacer capturas de estos resultados para el informe)
-- =====================================================================

-- a) La columna existe y las filas previas fueron etiquetadas
SELECT coleccion_id, COUNT(*) AS total_chunks
FROM normativa_bancaria
GROUP BY coleccion_id
ORDER BY total_chunks DESC;

-- b) El indice B-Tree existe
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'normativa_bancaria' AND indexname = 'idx_normativa_coleccion_id';

-- c) La funcion RPC existe con su firma
SELECT proname, pg_get_function_arguments(oid) AS argumentos
FROM pg_proc
WHERE proname = 'match_normativa_coleccion';
