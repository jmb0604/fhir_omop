select
    c.condition_source_value as snomed_code,
	con.concept_name,
    count(distinct c.person_id) as patient_count,
    count(*) as total_occurrences
from {{ ref('omop_condition_occurrence') }} c
left join {{ ref('omop_concept') }} con
on c.condition_source_value = con.concept_code
group by c.condition_source_value, con.concept_name
order by patient_count desc