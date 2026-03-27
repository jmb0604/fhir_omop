{{ config(
    materialized='incremental',
    unique_key='person_id'
) }}

with source_data as (
    select
        id as person_id,
        case
            when lower(gender) = 'male' then 8507
            when lower(gender) = 'female' then 8532
            else 0
        end as gender_concept_id,
        extract(year from birth_date) as year_of_birth,
        extract(month from birth_date) as month_of_birth,
        extract(day from birth_date) as day_of_birth,
        birth_date,
        gender as gender_source_value
    from {{ ref('stg_patients') }}
)

select *
from source_data sd

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t2
    where t2.person_id = sd.person_id
)
{% endif %}