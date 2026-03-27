{{ config(
    materialized='incremental',
    unique_key='concept_id'
) }}

with source_data as (
    select
        concept_id,
        concept_name,
        domain_id,
        vocabulary_id,
        concept_class_id,
        standard_concept,
        concept_code,
        to_date(valid_start_date::text, 'YYYYMMDD') as valid_start_date,
        to_date(valid_end_date::text, 'YYYYMMDD') as valid_end_date,
        invalid_reason
    from {{ ref('concept') }}
)

select *
from source_data sd

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t2
    where t2.concept_id = sd.concept_id
)
{% endif %}