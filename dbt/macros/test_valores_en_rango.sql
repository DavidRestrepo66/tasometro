{#- Fails for every non-null value outside [min_value, max_value]. -#}
{% test valores_en_rango(model, column_name, min_value, max_value) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
