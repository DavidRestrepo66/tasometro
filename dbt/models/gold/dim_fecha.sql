-- One row per weekly cut. A cut (a Friday) covers the 7 days ending on it; its
-- TPM is the daily average of the policy rate over those days, and the flag
-- marks weeks in which the rate changed.
with cortes as (
    select distinct fecha_corte from {{ ref('silver_tasas') }}
),

tpm as (
    select
        c.fecha_corte,
        avg(d.tpm) as tpm_semana,
        count(distinct d.tpm) > 1 as semana_con_cambio_tpm,
        count(d.fecha) as dias_con_tpm
    from cortes as c
    left join {{ ref('silver_tpm_diaria') }} as d
        on d.fecha between c.fecha_corte - 6 and c.fecha_corte
    group by c.fecha_corte
)

select
    fecha_corte,
    fecha_corte - 6 as semana_desde,
    extract(isoyear from fecha_corte)::integer as anio_iso,
    extract(week from fecha_corte)::integer as semana_iso,
    extract(year from fecha_corte)::integer as anio,
    extract(month from fecha_corte)::integer as mes,
    round(tpm_semana, 4) as tpm_semana,
    semana_con_cambio_tpm,
    dias_con_tpm
from tpm
