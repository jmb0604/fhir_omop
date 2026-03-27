{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with source_data as (
    select 
        "resourceType" as resource_type,
        id,
        name,
        telecom,
        gender,
        cast("birthDate" as date) as birth_date,
        address,
        "multipleBirthBoolean" as multiple_birth_boolean,
        contact,
        communication,
        "maritalStatus_coding" as marital_status_coding,
        "maritalStatus_text" as marital_status_text,
        "multipleBirthInteger" as multiple_birth_integer
    from {{ source('raw_data', 'raw_patients') }}
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