{{ config(severity='warn') }}
-- Zero-rate placeholders are about a quarter of the source rows. A cut whose
-- share moves more than 5 points from the average of its previous 8 cuts
-- suggests the source changed how it reports rates.
with por_corte as (
    select
        coalesce(t.fecha_corte, z.fecha_corte) as fecha_corte,
        coalesce(z.n_cero, 0)::numeric
            / nullif(coalesce(z.n_cero, 0) + coalesce(t.n_tasa, 0), 0) as share_cero
    from (
        select fecha_corte, sum(n_registros) as n_tasa
        from {{ ref('silver_tasas') }} group by 1
    ) as t
    full outer join (
        select fecha_corte, sum(n_tasa_cero) as n_cero
        from {{ ref('silver_tasas_cero') }} group by 1
    ) as z on z.fecha_corte = t.fecha_corte
),

con_historia as (
    select
        fecha_corte,
        share_cero,
        avg(share_cero) over (
            order by fecha_corte rows between 8 preceding and 1 preceding
        ) as share_previo
    from por_corte
)

select fecha_corte, round(share_cero, 4) as share_cero, round(share_previo, 4) as share_previo
from con_historia
where abs(share_cero - share_previo) > 0.05
