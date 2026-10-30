-- Every entity that reported rates must exist in the SFC catalog under the
-- composite key (tipo_entidad, codigo_entidad).
select distinct t.tipo_entidad, t.codigo_entidad, t.nombre_entidad
from {{ ref('silver_tasas') }} as t
left join {{ ref('silver_entidades') }} as e
    on e.tipo_entidad = t.tipo_entidad and e.codigo_entidad = t.codigo_entidad
where e.codigo_entidad is null
