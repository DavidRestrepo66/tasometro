{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='fecha_corte',
    )
}}

-- Zero-rate placeholder counts of the latest extraction of each cut, on the
-- same grain as silver_tasas. Feeds the zero-rate share drift test.
with lotes as (
    select * from {{ ref('stg_sfc__lotes_vigentes') }}
    where kind = 'zero_rates'
    {% if is_incremental() %}
      and fecha_corte in (
          select fecha_corte from {{ source('raw', 'sfc_lotes') }}
          where kind = 'zero_rates'
            and loaded_at > (select coalesce(max(_lote_loaded_at), '-infinity') from {{ this }})
      )
    {% endif %}
)

select
    l.fecha_corte,
    z.tipo_entidad::integer as tipo_entidad,
    z.codigo_entidad::integer as codigo_entidad,
    z.tipo_de_persona,
    z.sexo,
    z.tama_o_de_empresa as tamano_empresa,
    z.tipo_de_cr_dito as tipo_credito,
    z.producto_de_cr_dito as producto_credito,
    coalesce(z.plazo_de_cr_dito, 'N/A') as plazo_raw,
    z.tipo_de_tasa,
    z.n_tasa_cero::integer as n_tasa_cero,
    l.run_id as _run_id,
    l.loaded_at as _lote_loaded_at
from lotes as l
inner join {{ source('raw', 'sfc_tasas_cero') }} as z
    on z._source = l.source
    and z._fecha_corte = l.fecha_corte
    and z._run_id = l.run_id
    and z._tipo_entidad = l.tipo_entidad
    and z._codigo_entidad = l.codigo_entidad
