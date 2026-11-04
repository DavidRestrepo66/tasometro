-- Entities reporting in the current dataset must exist in the SFC catalog under
-- the composite key (tipo_entidad, codigo_entidad). Only entities absent from the
-- catalog that appear solely in the history dataset may be flagged otherwise.
select distinct t.tipo_entidad, t.codigo_entidad, t.nombre_entidad
from {{ ref('silver_tasas') }} as t
inner join {{ ref('silver_entidades') }} as e
    on e.tipo_entidad = t.tipo_entidad and e.codigo_entidad = t.codigo_entidad
where t._source = 'qzsc-9esp'
  and not e.en_catalogo
