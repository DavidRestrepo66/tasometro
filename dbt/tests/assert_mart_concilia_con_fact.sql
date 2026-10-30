-- The weekly mart keeps every peso of spread-eligible rates: per cut, its
-- disbursed amount equals the fact's for rate types with en_spread.
with fact as (
    select f.fecha_corte, sum(f.sum_montos) as montos
    from {{ ref('fact_tasas') }} as f
    inner join {{ ref('dim_producto') }} as p on p.producto_key = f.producto_key
    where p.en_spread
    group by 1
),

mart as (
    select fecha_corte, sum(sum_montos) as montos
    from {{ ref('mart_tasas_semanales') }}
    group by 1
)

select
    coalesce(f.fecha_corte, m.fecha_corte) as fecha_corte,
    f.montos as montos_fact,
    m.montos as montos_mart
from fact as f
full outer join mart as m on m.fecha_corte = f.fecha_corte
where f.montos is distinct from m.montos
