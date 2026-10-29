{#- Deterministic surrogate key from natural-key columns; nulls hash as '-'.
    Dimensions and the fact build keys with this same macro, so they match
    across rebuilds without a lookup. -#}
{% macro clave(columns) -%}
    md5(concat_ws('|'{% for c in columns %}, coalesce({{ c }}::text, '-'){% endfor %}))
{%- endmacro %}
