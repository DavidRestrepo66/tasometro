-- Freshness of the manual TPM seed: a cut newer than the last review date
-- means a BanRep change may be missing. Review the seed, then bump the
-- tpm_revisado_hasta var in dbt_project.yml.
select max(fecha_corte) as ultimo_corte
from {{ ref('silver_tasas') }}
having max(fecha_corte) > '{{ var("tpm_revisado_hasta") }}'::date
