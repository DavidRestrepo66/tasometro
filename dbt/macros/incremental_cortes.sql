{#
  Cuts whose silver rows must be rebuilt: those with a batch loaded after the
  silver watermark, taken from raw.sfc_lotes (not from the join output, so a
  re-extraction that yields no rows is still picked up). With
  rebuild_if_plazo_changed, every cut is rebuilt when the plazo_bandas seed no
  longer matches the bands stored in the target.
#}
{% macro cortes_a_reconstruir(kind, rebuild_if_plazo_changed=false) %}
    select fecha_corte from {{ source('raw', 'sfc_lotes') }}
    where kind = '{{ kind }}'
      and (
          loaded_at > (select coalesce(max(_lote_loaded_at), '-infinity') from {{ this }})
          {% if rebuild_if_plazo_changed %}
          or exists (
              select 1
              from {{ this }} as s
              left join {{ ref('plazo_bandas') }} as p on p.plazo_raw = s.plazo_raw
              where (s.plazo_banda, s.plazo_banda_orden, s.plazo_es_aproximado)
                  is distinct from (p.plazo_banda, p.plazo_banda_orden, p.es_aproximado)
          )
          {% endif %}
      )
{% endmacro %}

{#
  Incremental strategy: like delete+insert on fecha_corte, but the delete
  covers every cut selected by cortes_a_reconstruir. Config: lote_kind and
  optional rebuild_if_plazo_changed. The delete runs inside the same statement
  batch as the insert, after the temp relation (and its watermark) exists.
#}
{% macro get_incremental_delete_insert_cortes_sql(arg_dict) %}
    {%- set target = arg_dict['target_relation'] -%}
    {%- set tmp = arg_dict['temp_relation'] -%}
    {%- set cols = arg_dict['dest_columns'] | map(attribute='quoted') | join(', ') -%}

    delete from {{ target }}
    where fecha_corte in (
        {{ cortes_a_reconstruir(config.require('lote_kind'), config.get('rebuild_if_plazo_changed', false)) }}
    );

    insert into {{ target }} ({{ cols }})
    select {{ cols }} from {{ tmp }}
{% endmacro %}
