select
    person_id,
    gender_source_value,
    year_of_birth,
    extract(year from current_date) - year_of_birth as age
from {{ ref('omop_person') }}