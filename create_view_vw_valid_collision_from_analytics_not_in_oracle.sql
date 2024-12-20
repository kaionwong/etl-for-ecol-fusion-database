CREATE OR REPLACE VIEW vw_valid_collision_from_analytics_not_in_oracle AS
SELECT
    id,
    case_nbr
FROM public.analytics_collisions
WHERE 1=1
    AND id < 1  -- Filter for records with id less than 1; this may not be necessary as all collisions in 2015 or ealier have negative collision_id
ORDER BY id ASC;
