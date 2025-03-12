-- Query to show crime requirements and their details
SELECT DISTINCT
  c.difficulty as `Difficulty`,
  c.name as `Name`,
  c.slots_position as `Position`,
  item.name as `Item Needed`,
  c.slots_item_requirement_is_reusable as `Reusable Item`
FROM `torncity-402423.torn_data.v2_faction_crimes` c
LEFT JOIN `torncity-402423.torn_data.v2_torn_items` item ON item.id = c.slots_item_requirement_id
ORDER BY
  c.difficulty ASC,
  c.name ASC,
  c.slots_position ASC 