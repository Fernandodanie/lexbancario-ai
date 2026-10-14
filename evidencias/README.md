# Evidencias para el informe

Capturas sugeridas (nombres ordenados por paso). Las imágenes no se suben a Git (`*.png` está en .gitignore).

## Paso 1 - Aislamiento vectorial
- paso1_columna.png   -> consulta a information_schema.columns (coleccion_id VARCHAR(50))
- paso1_indice.png    -> pg_indexes (idx_normativa_coleccion_id, btree)
- paso1_funcion.png   -> pg_proc (match_normativa_coleccion)
- paso1_aislamiento_inicio.png / paso1_aislamiento_fin.png -> scripts/test_aislamiento.py (CERO CONTAMINACION CRUZADA)

## Paso 2 - /api/consultar
- paso2_coleccion_vacia.png -> coleccion_id "inexistente" (200, sin antecedentes)
- paso2_sin_coleccion.png   -> sin coleccion_id (422 Field required)

## Paso 3 - /api/ingestar-web
- paso3_ingesta_request.png  -> peticion en /docs (coleccion_id, urls, workers)
- paso3_ingesta_response.png -> respuesta (urls_ok, tiempo_total_s, desglose, rate_limit)
- paso3_logs_uvicorn.png     -> terminal del servidor (incluye pausas por 429 si ocurren)
- paso3_estado_coleccion.png -> GET /api/coleccion/{id} (chunks guardados)
- paso3_url_caida.png        -> una URL invalida/404 reportada como error sin abortar el lote
