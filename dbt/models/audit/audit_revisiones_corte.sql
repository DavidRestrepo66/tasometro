-- Revision audit (no SCD2): every re-extraction of a (cut, entity, kind) is
-- compared with the extraction just before it. Banks revise recent weeks, so
-- a changed content hash means the SFC published different numbers for a cut
-- we already had. Amount totals exist only for rate batches.
with totales as (
    select
        _source as source,
        _fecha_corte as fecha_corte,
        _run_id as run_id,
        _tipo_entidad as tipo_entidad,
        _codigo_entidad as codigo_entidad,
        sum(sum_montos::numeric) as sum_montos,
        sum(sum_creditos::numeric) as sum_creditos
    from {{ source('raw', 'sfc_tasas') }}
    group by 1, 2, 3, 4, 5
),

extracciones as (
    select
        l.source,
        l.fecha_corte,
        l.tipo_entidad::integer as tipo_entidad,
        l.codigo_entidad::integer as codigo_entidad,
        l.kind,
        l.run_id,
        l.extracted_at,
        l.row_count,
        l.rows_sha256,
        t.sum_montos,
        t.sum_creditos
    from {{ source('raw', 'sfc_lotes') }} as l
    left join totales as t
        on t.source = l.source
        and t.fecha_corte = l.fecha_corte
        and t.run_id = l.run_id
        and t.tipo_entidad = l.tipo_entidad
        and t.codigo_entidad = l.codigo_entidad
),

comparadas as (
    select
        *,
        lag(run_id) over serie as run_id_anterior,
        lag(extracted_at) over serie as extracted_at_anterior,
        lag(row_count) over serie as row_count_anterior,
        lag(rows_sha256) over serie as rows_sha256_anterior,
        lag(sum_montos) over serie as sum_montos_anterior,
        lag(sum_creditos) over serie as sum_creditos_anterior
    from extracciones
    window serie as (
        partition by fecha_corte, tipo_entidad, codigo_entidad, kind
        order by extracted_at, run_id
    )
)

select
    fecha_corte,
    tipo_entidad,
    codigo_entidad,
    kind,
    run_id,
    run_id_anterior,
    extracted_at,
    extracted_at_anterior,
    row_count - row_count_anterior as delta_filas,
    sum_montos - sum_montos_anterior as delta_montos,
    sum_creditos - sum_creditos_anterior as delta_creditos,
    rows_sha256 is distinct from rows_sha256_anterior as hubo_revision
from comparadas
where run_id_anterior is not null
