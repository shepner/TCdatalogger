-- Query to show crime rewards by difficulty
SELECT 
  c.difficulty,
  c.name,
  ROUND(AVG(c.rewards_respect), 2) as avg_respect,
  COUNT(DISTINCT c.slots_position) as num_slots,
  ROUND(AVG(c.rewards_money), 2) as avg_money
FROM `torncity-402423.torn_data.v2_faction_crimes` c
GROUP BY 
  c.difficulty,
  c.name
ORDER BY 
  c.difficulty ASC,
  c.name ASC 