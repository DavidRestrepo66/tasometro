-- Weekly rate of each bank per credit type, product and term band, against the
-- policy rate. Only rate types comparable with the TPM enter (en_spread);
-- segments are summed. The weighted rate is recomputed from additive sums.
with base as (
    select
        f.fecha_corte,
        f.entidad_key,
        p.tipo_credito,
        p.producto_credito,
        p.plazo_banda,
        p.plazo_banda_orden,
        sum(f.sum_montos) as sum_montos,
        sum(f.sum_creditos) as sum_creditos,
        sum(f.sum_tasa_x_monto) as sum_tasa_x_monto,
        bool_or(p.tasa_es_ambigua) as incluye_tasa_ambigua
    from {{ ref('fact_tasas') }} as f
    inner join {{ ref('dim_producto') }} as p on p.producto_key = f.producto_key
    where p.en_spread
    group by 1, 2, 3, 4, 5, 6
),

tasas as (
    select
        b.*,
        b.sum_tasa_x_monto / nullif(b.sum_montos, 0) as tasa_ponderada,
        sum(b.sum_tasa_x_monto) over mercado
            / nullif(sum(b.sum_montos) over mercado, 0) as tasa_mercado
    from base as b
    window mercado as (partition by b.fecha_corte, b.tipo_credito, b.producto_credito, b.plazo_banda)
),

con_tpm as (
    select
        t.*,
        d.tpm_semana,
        d.semana_con_cambio_tpm,
        lag(t.fecha_corte) over serie as fecha_corte_anterior,
        lag(t.tasa_ponderada) over serie as tasa_ponderada_anterior
    from tasas as t
    inner join {{ ref('dim_fecha') }} as d on d.fecha_corte = t.fecha_corte
    window serie as (
        partition by t.entidad_key, t.tipo_credito, t.producto_credito, t.plazo_banda
        order by t.fecha_corte
    )
)

select
    fecha_corte,
    entidad_key,
    tipo_credito,
    producto_credito,
    plazo_banda,
    plazo_banda_orden,
    sum_montos,
    sum_creditos,
    sum_tasa_x_monto,
    round(tasa_ponderada, 4) as tasa_ponderada,
    tpm_semana,
    round(tasa_ponderada - tpm_semana, 4) as spread,
    -- Change against the previous week only; a gap in the series gives null.
    case when fecha_corte_anterior = fecha_corte - 7
        then round(tasa_ponderada - tasa_ponderada_anterior, 4)
    end as variacion_semanal,
    round(tasa_mercado, 4) as tasa_mercado,
    round(tasa_ponderada - tasa_mercado, 4) as desvio_mercado,
    semana_con_cambio_tpm,
    incluye_tasa_ambigua
from con_tpm
