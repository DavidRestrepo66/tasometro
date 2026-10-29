{{ config(severity='warn') }}
-- Warns when the SFC revised a cut we had already loaded. Not an error: the
-- newest extraction feeds silver; this keeps the revision visible.
select *
from {{ ref('audit_revisiones_corte') }}
where hubo_revision
