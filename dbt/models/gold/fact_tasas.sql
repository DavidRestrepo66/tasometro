{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='fecha_corte',
    )
}}

-- Grain: week x entity x product x term x rate type x segment (silver grain).
-- Measures stay additive; rates are derived at query time as
-- sum(sum_tasa_x_monto) / sum(sum_montos).
select
    s.tasa_id,
    s.fecha_corte,
    {{ clave(['s.tipo_entidad', 's.codigo_entidad']) }} as entidad_key,
    {{ clave(['s.tipo_credito', 's.producto_credito', 's.plazo_raw', 's.tipo_de_tasa']) }}
        as producto_key,
    {{ clave(['s.tipo_de_persona', 's.sexo', 's.tamano_empresa']) }} as segmento_key,
    s.sum_montos,
    s.sum_creditos,
    s.sum_tasa_x_monto,
    s.tasa_ponderada,
    s.tasa_promedio_simple,
    s.tasa_minima,
    s.tasa_maxima,
    s.n_registros,
    s._lote_loaded_at
from {{ ref('silver_tasas') }} as s
{% if is_incremental() %}
where s.fecha_corte in (
    select fecha_corte from {{ ref('silver_tasas') }}
    where _lote_loaded_at > (select coalesce(max(_lote_loaded_at), '-infinity') from {{ this }})
)
{% endif %}
