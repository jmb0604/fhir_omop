select
    extract(year from current_date) - year_of_birth as age,
    count(*) as patient_count
from {{ ref('omop_person') }}
group by age
order by age