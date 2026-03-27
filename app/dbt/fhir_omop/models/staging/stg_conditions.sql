{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with source_data as (
    select 
        "resourceType" as resource_type,
        id,
        cast("onsetDateTime" as timestamp) as onset_date_time,
        cast("recordedDate" as timestamp) as recorded_date,
        "clinicalStatus_coding" as clinical_status_coding,
        "verificationStatus_coding" as verification_status_coding,
        code_coding,
        code_text,
        replace(subject_reference, 'urn:uuid:', '') as subject_reference,
        replace(encounter_reference, 'urn:uuid:', '') as encounter_reference,
        cast("abatementDateTime" as timestamp) as abatement_date_time
    from {{ source('raw_data', 'raw_conditions') }}
)

select *
from source_data sd

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t2
    where t2.id = sd.id
)
{% endif %}