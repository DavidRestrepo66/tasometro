{{ config(materialized='table') }}

-- Policy rate in force on every day, from the first seeded change until the
-- later of today and the latest cut.
with limites as (
    select
        min(c.fecha_cambio) as desde,
        greatest(current_date, (select max(fecha_corte) from {{ ref('silver_tasas') }})) as hasta
    from {{ ref('tpm_cambios') }} as c
),

dias as (
    select generate_series(desde::timestamp, hasta::timestamp, interval '1 day')::date as fecha
    from limites
)

select
    d.fecha,
    vigente.tpm,
    vigente.fecha_cambio as vigente_desde
from dias as d
cross join lateral (
    select c.tpm, c.fecha_cambio
    from {{ ref('tpm_cambios') }} as c
    where c.fecha_cambio <= d.fecha
    order by c.fecha_cambio desc
    limit 1
) as vigente
