-- Query to show crime difficulty levels and rewards with aggregations
WITH SingleCrimeInstance AS (
  SELECT 
    ARRAY_AGG(id ORDER BY id LIMIT 1)[OFFSET(0)] as id,
    name,
    ANY_VALUE(difficulty) as difficulty
  FROM `torncity-402423.torn_data.v2_faction_crimes`
  GROUP BY name
)
SELECT
  sci.difficulty as `Difficulty`,
  sci.name as `Name`,
  ROUND(AVG(c.rewards_respect), 0) as `Avg respect`,
  ROUND(AVG(TIMESTAMP_DIFF(c.ready_at, c.planning_at, HOUR)) / 24.0, 0) as `Days`,
  (SELECT COUNT(slots_position)
   FROM `torncity-402423.torn_data.v2_faction_crimes` c2
   WHERE c2.id = sci.id) as `Positions`,
  ROUND(AVG(c.rewards_money), 0) as `Average reward`,
  ROUND(AVG(c.rewards_money) / NULLIF((SELECT COUNT(slots_position)
   FROM `torncity-402423.torn_data.v2_faction_crimes` c2
   WHERE c2.id = sci.id), 0), 0) as `Avg pay per position`
FROM SingleCrimeInstance sci
JOIN `torncity-402423.torn_data.v2_faction_crimes` c ON c.name = sci.name
GROUP BY
  sci.difficulty,
  sci.name,
  sci.id
ORDER BY
  sci.difficulty ASC,
  sci.name ASC 