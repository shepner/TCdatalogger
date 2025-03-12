-- Diagnostic query to check data in both tables
SELECT 
  'Members' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT id) as distinct_ids,
  COUNT(DISTINCT name) as distinct_names
FROM `torncity-402423.torn_data.v2_faction_members`
UNION ALL
SELECT
  'Crimes' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT slots_user_id) as distinct_ids,
  COUNT(DISTINCT name) as distinct_names
FROM `torncity-402423.torn_data.v2_faction_crimes`
WHERE slots_user_id IS NOT NULL;

-- Sample of members data
SELECT 
  id,
  name,
  TIMESTAMP_MILLIS(CAST(last_action AS INT64)) as last_action
FROM `torncity-402423.torn_data.v2_faction_members`
LIMIT 5;

-- Sample of crimes data with user assignments
SELECT 
  slots_user_id,
  name,
  difficulty,
  status,
  slots_crime_pass_rate
FROM `torncity-402423.torn_data.v2_faction_crimes`
WHERE slots_user_id IS NOT NULL
LIMIT 5; 