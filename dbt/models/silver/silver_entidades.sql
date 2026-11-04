{{ config(materialized='table') }}

-- SFC supervised entities, keyed by (tipo_entidad, codigo_entidad). The catalog
-- only lists current entities, so entities that reported rates in the past and
-- are absent from it are added from the rates themselves (latest name, no NIT) and
-- flagged with en_catalogo = false.
with catalogo as (
    select
        tipo_entidad::integer as tipo_entidad,
        cod_entidad::integer as codigo_entidad,
        razon_social,
        numeroidentificacion as nit,
        true as en_catalogo
    from {{ source('raw', 'sfc_entidades') }}
),

historicas as (
    select distinct on (t.tipo_entidad, t.codigo_entidad)
        t.tipo_entidad,
        t.codigo_entidad,
        t.nombre_entidad as razon_social,
        null::text as nit,
        false as en_catalogo
    from {{ ref('silver_tasas') }} as t
    left join catalogo as c
        on c.tipo_entidad = t.tipo_entidad and c.codigo_entidad = t.codigo_entidad
    where c.codigo_entidad is null
    order by t.tipo_entidad, t.codigo_entidad, t.fecha_corte desc
)

select * from catalogo
union all
select * from historicas
