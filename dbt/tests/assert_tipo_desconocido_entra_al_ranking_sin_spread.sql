-- Rates of unknown type (the history dataset reports 'N/A') appear in the
-- weekly mart but never get a spread; every other row has one. The mart must
-- carry them whenever the fact table holds any.
select fecha_corte, entidad_key, tasa_desconocida, spread
from {{ ref('mart_tasas_semanales') }}
where tasa_desconocida is distinct from (spread is null)

union all

select null::date, null, true, null
from (select 1) as sin_filas
where exists (
    select 1
    from {{ ref('fact_tasas') }} as f
    inner join {{ ref('dim_producto') }} as p on p.producto_key = f.producto_key
    where p.clase_tasa = 'Desconocida'
)
and not exists (
    select 1 from {{ ref('mart_tasas_semanales') }} where tasa_desconocida
)
