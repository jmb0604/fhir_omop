{{ config(
    materialized='incremental',
    unique_key='condition_occurrence_id'
) }}

with source_data as (
    select
        id as condition_occurrence_id,
        subject_reference as person_id,
        null as condition_concept_id,
        onset_date_time::date as condition_start_date,
        onset_date_time::time as condition_start_date_time,
        abatement_date_time::date as condition_end_date,
        abatement_date_time::time as condition_end_date_time,
        32840 as condition_type_concept_id, -- EHR problem list
        case 
            when code_coding -> 0 ->> 'code' = 'active' then 9181
            when code_coding -> 0 ->> 'code' = 'resolved' then 37109701
            else 0
        end as condition_status_concept_id,
        code_coding -> 0 ->> 'code' as condition_source_value
    from {{ ref('stg_conditions') }}
)

select *
from source_data sd

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t2
    where t2.condition_occurrence_id = sd.condition_occurrence_id
)
{% endif %}