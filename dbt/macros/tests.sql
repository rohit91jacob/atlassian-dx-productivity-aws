{#- Generic test: numeric column within [min_value, max_value] (nulls pass). -#}
{% test between(model, column_name, min_value, max_value) %}
select * from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}

{#- Generic test: column_a <= column_b whenever both are present. -#}
{% test not_after(model, column_name, other_column) %}
select * from {{ model }}
where {{ column_name }} is not null and {{ other_column }} is not null
  and {{ column_name }} > {{ other_column }}
{% endtest %}

{#- Generic model-level test: the combination of columns is unique (no dbt_utils needed). -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
