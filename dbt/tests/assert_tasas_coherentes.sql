-- Rate bounds are coherent within every group.
select tasa_id
from {{ ref('silver_tasas') }}
where tasa_minima > tasa_maxima
   or tasa_ponderada < tasa_minima - 1e-6
   or tasa_ponderada > tasa_maxima + 1e-6
