-- Junk dimension of the borrower segment: person type, sex and company size.
-- Sex applies to natural persons only; legal persons carry 'No aplica'.
select distinct
    {{ clave(['tipo_de_persona', 'sexo', 'tamano_empresa']) }} as segmento_key,
    tipo_de_persona,
    sexo,
    tamano_empresa
from {{ ref('silver_tasas') }}
