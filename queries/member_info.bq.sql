-- Query to show member success rates per crime difficulty
WITH RecentActivity AS (
  SELECT 
    m.id,
    COUNT(DISTINCT CASE WHEN TIMESTAMP_DIFF(c.server_timestamp, c.executed_at, DAY) <= 30 THEN c.id END) as recent_participations_30d,
    COUNT(DISTINCT CASE WHEN TIMESTAMP_DIFF(c.server_timestamp, c.executed_at, DAY) <= 7 THEN c.id END) as recent_participations_7d
  FROM `torncity-402423.torn_data.v2_faction_40832_members` AS m
  LEFT JOIN `torncity-402423.torn_data.v2_faction_40832_crimes` AS c 
    ON c.slots_user_id = m.id
    AND c.status = 'Successful'
    AND TIMESTAMP_DIFF(c.server_timestamp, c.executed_at, DAY) <= 30
  GROUP BY m.id
),
MemberSuccesses AS (
  SELECT 
    m.id as id,
    m.name,
    m.level,
    m.days_in_faction,
    m.position,
    TIMESTAMP_DIFF(m.server_timestamp, m.last_action_timestamp, DAY) as days_since_last_action,
    m.is_in_oc,
    COALESCE(ra.recent_participations_30d, 0) as recent_participations_30d,
    COALESCE(ra.recent_participations_7d, 0) as recent_participations_7d,
    COALESCE(c.difficulty, 0) as difficulty,
    COALESCE(MAX(c.slots_crime_pass_rate), 0) as max_success_rate,
    FIRST_VALUE(CASE 
      WHEN MAX(c.slots_crime_pass_rate) >= 80 THEN GREATEST(c.difficulty, 1)
      ELSE GREATEST(COALESCE(c.difficulty - 1, 1), 1)
    END) OVER (PARTITION BY m.id ORDER BY c.difficulty DESC) as recommended_difficulty
  FROM `torncity-402423.torn_data.v2_faction_40832_members` AS m
  LEFT JOIN `torncity-402423.torn_data.v2_faction_40832_crimes` AS c 
    ON c.slots_user_id = m.id
    AND c.status = 'Successful'
  LEFT JOIN RecentActivity AS ra
    ON ra.id = m.id
  GROUP BY 
    m.id,
    m.name,
    m.level,
    m.days_in_faction,
    m.position,
    m.server_timestamp,
    m.last_action_timestamp,
    m.is_in_oc,
    ra.recent_participations_30d,
    ra.recent_participations_7d,
    c.difficulty
)
SELECT *
FROM (
  SELECT 
    name as `Name`,
    level as `Level`,
    days_in_faction as `Days in Faction`,
    position as `Position`,
    days_since_last_action as `Days Inactive`,
    is_in_oc as `In OC`,
    recent_participations_30d as `30 Day OCs`,
    recent_participations_7d as `7 Day OCs`,
    GREATEST(COALESCE(recommended_difficulty, 1), 1) as `Max Recommended OC`,
    CAST(difficulty AS STRING) as difficulty,
    max_success_rate,
    CONCAT('https://www.torn.com/profiles.php?XID=', CAST(id as STRING)) as `Profile URL`
  FROM MemberSuccesses
)
PIVOT (
  MAX(max_success_rate)
  FOR difficulty IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')
)
ORDER BY 
  `Max Recommended OC` DESC,
  `In OC` ASC,
  `7 Day OCs` ASC,
  `30 Day OCs` ASC,
  `Level` DESC,
  `Days in Faction` DESC,
  `Position` ASC,
  `Days Inactive` DESC,
  `Name` ASC 
