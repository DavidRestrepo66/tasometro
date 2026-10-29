{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='fecha_corte',
    )
}}

-- Typed source-aggregated rates of the latest extraction of each cut.
-- Incremental runs rebuild every cut that received a batch since the last
-- build, so the weekly 8-week re-extraction is reprocessed whole.
with lotes as (
    select * from {{ ref('stg_sfc__lotes_vigentes') }}
    where kind = 'rates'
    {% if is_incremental() %}
      and fecha_corte in (
          select fecha_corte from {{ source('raw', 'sfc_lotes') }}
          where kind = 'rates'
            and loaded_at > (select coalesce(max(_lote_loaded_at), '-infinity') from {{ this }})
      )
    {% endif %}
),

tasas as (
    select
        l.fecha_corte,
        t.tipo_entidad::integer as tipo_entidad,
        t.codigo_entidad::integer as codigo_entidad,
        t.nombre_entidad,
        t.tipo_de_persona,
        t.sexo,
        t.tama_o_de_empresa as tamano_empresa,
        t.tipo_de_cr_dito as tipo_credito,
        t.producto_de_cr_dito as producto_credito,
        coalesce(t.plazo_de_cr_dito, 'N/A') as plazo_raw,
        t.tipo_de_tasa,
        t.sum_montos::numeric as sum_montos,
        t.sum_creditos::numeric as sum_creditos,
        t.sum_tasa_x_monto::numeric as sum_tasa_x_monto,
        t.avg_tasa_simple::numeric as tasa_promedio_simple,
        t.min_tasa::numeric as tasa_minima,
        t.max_tasa::numeric as tasa_maxima,
        t.n_registros::integer as n_registros,
        l.source as _source,
        l.run_id as _run_id,
        l.extracted_at as _extracted_at,
        l.loaded_at as _lote_loaded_at
    from lotes as l
    inner join {{ source('raw', 'sfc_tasas') }} as t
        on t._source = l.source
        and t._fecha_corte = l.fecha_corte
        and t._run_id = l.run_id
        and t._tipo_entidad = l.tipo_entidad
        and t._codigo_entidad = l.codigo_entidad
)

select
    md5(concat_ws('|',
        t.fecha_corte, t.tipo_entidad, t.codigo_entidad,
        coalesce(t.tipo_de_persona, '-'), coalesce(t.sexo, '-'), coalesce(t.tamano_empresa, '-'),
        coalesce(t.tipo_credito, '-'), coalesce(t.producto_credito, '-'),
        t.plazo_raw, coalesce(t.tipo_de_tasa, '-')
    )) as tasa_id,
    t.fecha_corte,
    t.tipo_entidad,
    t.codigo_entidad,
    t.nombre_entidad,
    t.tipo_de_persona,
    t.sexo,
    t.tamano_empresa,
    t.tipo_credito,
    t.producto_credito,
    t.plazo_raw,
    p.plazo_banda,
    p.plazo_banda_orden,
    p.es_aproximado as plazo_es_aproximado,
    t.tipo_de_tasa,
    t.sum_montos,
    t.sum_creditos,
    t.sum_tasa_x_monto,
    t.sum_tasa_x_monto / nullif(t.sum_montos, 0) as tasa_ponderada,
    t.tasa_promedio_simple,
    t.tasa_minima,
    t.tasa_maxima,
    t.n_registros,
    t._source,
    t._run_id,
    t._extracted_at,
    t._lote_loaded_at
from tasas as t
left join {{ ref('plazo_bandas') }} as p on p.plazo_raw = t.plazo_raw
