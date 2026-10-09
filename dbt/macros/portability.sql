{#- Small cross-engine helpers so the same models compile for DuckDB (local/CI) and Athena
    (Trino). Prefer dbt's own cross-database macros (dbt.datediff, dbt.date_trunc, dbt.dateadd);
    these cover what dbt does not. -#}

{% macro percentile(expr, p) -%}
  {{ return(adapter.dispatch('percentile', 'dx')(expr, p)) }}
{%- endmacro %}

{% macro default__percentile(expr, p) -%}
  quantile_cont({{ expr }}, {{ p }})
{%- endmacro %}

{% macro athena__percentile(expr, p) -%}
  approx_percentile({{ expr }}, {{ p }})
{%- endmacro %}

{#- Elapsed hours between two timestamps as a double (never integer division). -#}
{% macro hours_between(start_ts, end_ts) -%}
  (cast({{ dbt.datediff(start_ts, end_ts, 'second') }} as double) / 3600.0)
{%- endmacro %}

{% macro days_between(start_ts, end_ts) -%}
  (cast({{ dbt.datediff(start_ts, end_ts, 'second') }} as double) / 86400.0)
{%- endmacro %}

{#- The run's evaluation instant (var as_of, 'YYYY-MM-DD HH:MM:SS' UTC). -#}
{% macro as_of_ts() -%}
  cast('{{ var("as_of") }}' as timestamp)
{%- endmacro %}

{% macro safe_ratio(numerator, denominator) -%}
  case when {{ denominator }} = 0 then null
       else cast({{ numerator }} as double) / cast({{ denominator }} as double) end
{%- endmacro %}

{% macro sql_list(values) -%}
  ({%- for v in values -%}'{{ v }}'{%- if not loop.last -%}, {% endif -%}{%- endfor -%})
{%- endmacro %}

{#- Integer part of a semantic-version component ('4.1.0' -> 4, 1, 0); null when absent. -#}
{% macro semver_part(expr, n) -%}
  try_cast(nullif(regexp_extract({{ expr }}, '^([0-9]+)\.([0-9]+)(?:\.([0-9]+))?', {{ n }}), '') as integer)
{%- endmacro %}

{#- Count of rows matching a condition, typed bigint on every engine (DuckDB widens sum() of
    integers to hugeint). -#}
{% macro count_when(condition) -%}
  cast(sum(case when {{ condition }} then 1 else 0 end) as bigint)
{%- endmacro %}

{% macro sum_bigint(expr) -%}
  cast(sum({{ expr }}) as bigint)
{%- endmacro %}
