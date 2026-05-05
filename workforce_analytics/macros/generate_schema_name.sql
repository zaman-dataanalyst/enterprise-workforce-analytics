{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        PURPOSE: 
        Prevents dbt from prefixing custom schemas with the target schema.
        Without this, dbt creates: silver_layer_silver.
        With this, dbt uses the exact name: silver_layer.
    #}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}