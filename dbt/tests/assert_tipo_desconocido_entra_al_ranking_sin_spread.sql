-- Rates of unknown type (the history dataset reports 'N/A') appear in the
-- weekly mart but never get a spread; every other row has one.
select fecha_corte, entidad_key, tasa_desconocida, spread
from {{ ref('mart_tasas_semanales') }}
where tasa_desconocida is distinct from (spread is null)

union all

select null::date, null, true, null
from (select 1) as sin_filas
where not exists (
    select 1 from {{ ref('mart_tasas_semanales') }} where tasa_desconocida
)
