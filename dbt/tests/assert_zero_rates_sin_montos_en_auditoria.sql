-- Amount totals exist only for rate batches: a zero_rates comparison must
-- never carry amount deltas.
select *
from {{ ref('audit_revisiones_corte') }}
where kind <> 'rates'
  and (delta_montos is not null or delta_creditos is not null)
