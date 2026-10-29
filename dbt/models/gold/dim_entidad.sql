-- One row per entity that reported rates, with its latest short name and the
-- catalog's legal name and NIT.
with nombres as (
    select distinct on (tipo_entidad, codigo_entidad)
        tipo_entidad, codigo_entidad, nombre_entidad
    from {{ ref('silver_tasas') }}
    order by tipo_entidad, codigo_entidad, fecha_corte desc
)

select
    {{ clave(['n.tipo_entidad', 'n.codigo_entidad']) }} as entidad_key,
    n.tipo_entidad,
    n.codigo_entidad,
    n.nombre_entidad,
    e.razon_social,
    e.nit
from nombres as n
left join {{ ref('silver_entidades') }} as e
    on e.tipo_entidad = n.tipo_entidad and e.codigo_entidad = n.codigo_entidad
