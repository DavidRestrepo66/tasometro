-- Credit product with its conditions: canonical term band and rate type.
-- One row per (credit type, product, raw term, rate type).
with productos as (
    select distinct
        tipo_credito, producto_credito, plazo_raw, plazo_banda, plazo_banda_orden,
        plazo_es_aproximado, tipo_de_tasa
    from {{ ref('silver_tasas') }}
)

select
    {{ clave(['p.tipo_credito', 'p.producto_credito', 'p.plazo_raw', 'p.tipo_de_tasa']) }}
        as producto_key,
    p.tipo_credito,
    p.producto_credito,
    p.plazo_raw,
    p.plazo_banda,
    p.plazo_banda_orden,
    p.plazo_es_aproximado,
    p.tipo_de_tasa,
    c.clase_tasa,
    c.en_spread,
    c.es_ambigua as tasa_es_ambigua
from productos as p
left join {{ ref('tipo_tasa_clases') }} as c on c.tipo_de_tasa = p.tipo_de_tasa
