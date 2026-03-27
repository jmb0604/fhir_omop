select
    p.gender_source_value as gender,
    c.condition_source_value as snomed_code,
	con.concept_name,
    count(*) as count
from {{ ref('omop_condition_occurrence') }} c
left join {{ ref('omop_concept') }} con
	on c.condition_source_value = con.concept_code
left join {{ ref('omop_person') }} p
    on p.person_id = c.person_id
group by p.gender_source_value, c.condition_source_value, con.concept_name
order by p.gender_source_value, count desc