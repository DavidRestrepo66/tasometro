-- The latest extraction of each (cut, entity, kind). Re-extractions of a cut
-- land as new runs; only the newest one feeds silver, older ones stay in raw
-- for the revision audit.
with ranked as (
    select
        source,
        fecha_corte,
        run_id,
        tipo_entidad,
        codigo_entidad,
        kind,
        row_count,
        rows_sha256,
        extracted_at,
        loaded_at,
        row_number() over (
            partition by fecha_corte, tipo_entidad, codigo_entidad, kind
            order by extracted_at desc
        ) as extraction_rank
    from {{ source('raw', 'sfc_lotes') }}
)

select * from ranked where extraction_rank = 1
