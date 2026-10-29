#!/usr/bin/env bash
# Regenerates dbt/ci/fixture from a populated local warehouse: two recent cuts
# and four entities, every loaded run. Needs the compose stack and .env.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a

CUTS="'2026-10-09','2026-10-16'"
ENTITIES="('1','30'),('1','23'),('1','42'),('1','13')"
OUT=dbt/ci/fixture

copy() { # name, query
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d warehouse \
    -c "\\copy ($2) to stdout with csv header" > "$OUT/$1.csv"
}

copy sfc_lotes "select * from raw.sfc_lotes where fecha_corte in ($CUTS)
  and (tipo_entidad, codigo_entidad) in ($ENTITIES) order by 1,2,3,4,5,6"
copy sfc_tasas "select * from raw.sfc_tasas where _fecha_corte in ($CUTS)
  and (_tipo_entidad, _codigo_entidad) in ($ENTITIES) order by _source,_fecha_corte,_run_id,_tipo_entidad,_codigo_entidad,tipo_de_persona,sexo,tama_o_de_empresa,tipo_de_cr_dito,producto_de_cr_dito,plazo_de_cr_dito,tipo_de_tasa"
copy sfc_tasas_cero "select * from raw.sfc_tasas_cero where _fecha_corte in ($CUTS)
  and (_tipo_entidad, _codigo_entidad) in ($ENTITIES) order by 1,2,3,4,5"
copy sfc_entidades "select * from raw.sfc_entidades
  where (tipo_entidad, cod_entidad) in ($ENTITIES) order by 1,2"
wc -l "$OUT"/*.csv
