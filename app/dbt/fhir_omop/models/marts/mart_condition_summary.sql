select 
    c.condition_source_value as snomed_code, 
    con.concept_name, 
    count(*) as count
from {{ ref('omop_condition_occurrence') }} c
left join {{ ref('omop_concept') }} con
on c.condition_source_value = con.concept_code
group by c.condition_source_value, con.concept_name
order by count desc