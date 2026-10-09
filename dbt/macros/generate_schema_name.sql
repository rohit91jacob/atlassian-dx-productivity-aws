{#- Use the configured schema name as-is (staging, intermediate, marts, reference) instead of
    dbt's default "<target schema>_<custom schema>", so tables land in predictable places.
    On Athena every schema is a Glue database, so they get a dx_ prefix (dx_marts, ...). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- elif target.type == 'athena' -%}
    {{ env_var('DX_ATHENA_SCHEMA_PREFIX', 'dx_') }}{{ custom_schema_name | trim }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
