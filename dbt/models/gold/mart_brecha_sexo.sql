-- Rate gap by sex within the same bank, product, term band and week, natural
-- persons only. Comparing inside one bank and product keeps portfolio mix out
-- of the gap. Descriptive, not causal. Only rate types comparable with the TPM.
with por_sexo as (
    select
        f.fecha_corte,
        f.entidad_key,
        p.tipo_credito,
        p.producto_credito,
        p.plazo_banda,
        s.sexo,
        sum(f.sum_montos) as sum_montos,
        sum(f.sum_creditos) as sum_creditos,
        sum(f.sum_tasa_x_monto) / nullif(sum(f.sum_montos), 0) as tasa_ponderada
    from {{ ref('fact_tasas') }} as f
    inner join {{ ref('dim_producto') }} as p on p.producto_key = f.producto_key
    inner join {{ ref('dim_segmento') }} as s on s.segmento_key = f.segmento_key
    where p.en_spread
      and s.tipo_de_persona = 'Natural'
      and s.sexo in ('Femenino', 'Masculino')
    group by 1, 2, 3, 4, 5, 6
)

select
    fecha_corte,
    entidad_key,
    tipo_credito,
    producto_credito,
    plazo_banda,
    round(max(tasa_ponderada) filter (where sexo = 'Femenino'), 4) as tasa_femenino,
    round(max(tasa_ponderada) filter (where sexo = 'Masculino'), 4) as tasa_masculino,
    round(
        max(tasa_ponderada) filter (where sexo = 'Femenino')
        - max(tasa_ponderada) filter (where sexo = 'Masculino'), 4
    ) as brecha_femenino_menos_masculino,
    max(sum_creditos) filter (where sexo = 'Femenino') as creditos_femenino,
    max(sum_creditos) filter (where sexo = 'Masculino') as creditos_masculino
from por_sexo
group by 1, 2, 3, 4, 5
-- Only cells where both sexes borrowed, so every gap compares like with like.
having count(distinct sexo) = 2
