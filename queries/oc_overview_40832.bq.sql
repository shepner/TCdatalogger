-- Query to show crime difficulty levels and rewards with aggregations
WITH SingleCrimeInstance AS (
  SELECT 
    ARRAY_AGG(id ORDER BY id DESC LIMIT 1)[OFFSET(0)] as id,
    name,
    ANY_VALUE(difficulty) as difficulty
  FROM `torncity-402423.torn_data.v2_faction_40832_crimes`
  GROUP BY name
),
PositionCounts AS (
  SELECT 
    name,
    id,
    COUNT(*) as position_count
  FROM `torncity-402423.torn_data.v2_faction_40832_crimes`
  GROUP BY name, id
),
RewardStats AS (
  SELECT
    name,
    id,
    FIRST_VALUE(rewards_respect) OVER (PARTITION BY name, id ORDER BY rewards_respect DESC) as instance_respect,
    FIRST_VALUE(rewards_money) OVER (PARTITION BY name, id ORDER BY rewards_money DESC) as instance_money
  FROM `torncity-402423.torn_data.v2_faction_40832_crimes`
  WHERE rewards_respect IS NOT NULL OR rewards_money IS NOT NULL
),
CrimeStats AS (
  SELECT
    sci.difficulty,
    sci.name,
    ROUND(AVG(rs.instance_respect), 0) as avg_respect,
    ROUND(AVG(CASE WHEN c.ready_at IS NOT NULL AND c.planning_at IS NOT NULL 
              THEN TIMESTAMP_DIFF(c.ready_at, c.planning_at, HOUR) END) / 24.0, 0) as days,
    MAX(pc.position_count) as positions,
    ROUND(AVG(rs.instance_money), 0) as avg_reward
  FROM SingleCrimeInstance sci
  JOIN `torncity-402423.torn_data.v2_faction_40832_crimes` c ON c.name = sci.name
  JOIN PositionCounts pc ON pc.name = c.name
  LEFT JOIN RewardStats rs ON rs.name = c.name AND rs.id = c.id
  GROUP BY
    sci.difficulty,
    sci.name
)
SELECT
  difficulty as `Difficulty`,
  name as `Name`,
  avg_respect as `Avg respect`,
  days as `Days`,
  positions as `Positions`,
  avg_reward as `Average reward`,
  ROUND(avg_reward / NULLIF(positions, 0), 0) as `Avg pay per position`,
  ROUND((avg_reward / NULLIF(positions, 0)) / NULLIF(days, 0), 0) as `Avg pay per position per day`
FROM CrimeStats
ORDER BY
  difficulty ASC,
  name ASC 