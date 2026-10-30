{{ config(materialized='table') }}

-- SFC supervised entities, keyed by (tipo_entidad, codigo_entidad).
select
    tipo_entidad::integer as tipo_entidad,
    cod_entidad::integer as codigo_entidad,
    razon_social,
    numeroidentificacion as nit
from {{ source('raw', 'sfc_entidades') }}
