-- Query to show faction crimes with member details for each slot
SELECT
  c.server_timestamp,
  c.crime_id,
  c.name,
  c.participants,
  c.time_started,
  c.time_completed,
  c.time_ready,
  c.initiated_by,
  c.planned_by,
  c.difficulty,
  c.status,
  c.slots_position,
  c.slots_user_id,
  c.slots_crime_pass_rate,
  m.name as member_name,
  m.level as member_level,
  m.days_in_faction as member_days_in_faction,
  m.position as member_position,
  m.last_action_timestamp as member_last_action,
  m.is_in_oc as member_in_oc
FROM `torncity-402423.torn_data.v2_faction_crimes` c
LEFT JOIN `torncity-402423.torn_data.v2_faction_members` m
  ON c.slots_user_id = m.id
ORDER BY 
  c.server_timestamp DESC,
  c.crime_id ASC,
  c.slots_position ASC 