SELECT 
  person_key AS picked_by_person_key
  , full_name AS picked_by_full_name
  , preferred_name AS picked_by_preferred_name
  , search_name AS picked_by_search_name
   , is_employee
FROM {{ref('dim_person')}}