-- The composite key is unique in the catalog, so joins never fan out.
select tipo_entidad, codigo_entidad, count(*) as n
from {{ ref('silver_entidades') }}
group by 1, 2
having count(*) > 1
