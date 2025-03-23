-- Query to show crime information with member names and item names
SELECT
  -- Basic Crime Information
  c.id as `Crime ID`,
  c.name as `Crime Name`,
  c.difficulty as `Difficulty`,
  c.status as `Status`,
  
  -- Timestamps
  c.created_at as `Started At`,
  c.planning_at as `Planning Started`,
  c.ready_at as `Ready At`,
  c.executed_at as `Executed At`,
  c.expired_at as `Expires At`,
  
  -- Slot Information
  c.slots_position as `Position`,
  COALESCE(m.name, 'Empty') as `Member Name`,
  c.slots_user_id as `Member ID`,
  c.slots_user_joined_at as `Member Joined At`,
  c.slots_user_progress as `Member Progress`,
  c.slots_success_chance as `Success Chance`,
  c.slots_crime_pass_rate as `Pass Rate`,
  
  -- Item Requirements
  COALESCE(item.name, 'None') as `Required Item`,
  c.slots_item_requirement_id as `Required Item ID`,
  c.slots_item_requirement_is_reusable as `Item is Reusable`,
  c.slots_item_requirement_is_available as `Item is Available`,
  
  -- Rewards
  c.rewards_money as `Money Reward`,
  c.rewards_respect as `Respect Reward`,
  c.rewards_items_id as `Reward Item ID`,
  c.rewards_items_quantity as `Reward Item Quantity`,
  
  -- Payout Information
  c.rewards_payout_type as `Payout Type`,
  c.rewards_payout_percentage as `Payout Percentage`,
  c.rewards_payout_paid_by as `Paid By ID`,
  c.rewards_payout_paid_at as `Paid At`,
  
  -- Links
  CONCAT('https://www.torn.com/profiles.php?XID=', CAST(c.slots_user_id as STRING)) as `Member Profile`
FROM `torncity-402423.torn_data.v2_faction_40832_crimes` c
LEFT JOIN `torncity-402423.torn_data.v2_faction_40832_members` m 
  ON m.id = c.slots_user_id
LEFT JOIN `torncity-402423.torn_data.v2_torn_items` item 
  ON item.id = c.slots_item_requirement_id
ORDER BY
  c.id ASC,
  c.slots_position ASC 