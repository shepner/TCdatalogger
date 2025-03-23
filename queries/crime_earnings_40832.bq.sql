-- Query to show crime earnings and money distribution between faction and participants
WITH CrimeEarnings AS (
  SELECT 
    DATE(rewards_payout_paid_at) as payout_date,
    id as crime_id,
    name as crime_name,
    rewards_money as total_money,
    -- 20% goes to faction when payout_type is 'balance'
    CASE 
      WHEN rewards_payout_type = 'balance' 
      THEN ROUND(rewards_money * (1 - rewards_payout_percentage/100.0), 0) 
      ELSE 0 
    END as faction_money,
    -- 80% goes to participants when payout_type is 'balance'
    CASE 
      WHEN rewards_payout_type = 'balance' 
      THEN ROUND(rewards_money * (rewards_payout_percentage/100.0), 0) 
      ELSE rewards_money 
    END as participant_money
  FROM `torncity-402423.torn_data.v2_faction_40832_crimes`
  WHERE 
    status = 'Successful'
    AND rewards_payout_paid_at IS NOT NULL
    AND rewards_money > 0
)
SELECT
  payout_date as Payout_Date,
  crime_id as Crime_ID,
  crime_name as Crime_Name,
  CAST(SUM(total_money) AS STRING) as Total_Money,
  CAST(SUM(faction_money) AS STRING) as Faction_Money,
  CAST(SUM(participant_money) AS STRING) as Participant_Money
FROM CrimeEarnings
GROUP BY 
  payout_date,
  crime_id,
  crime_name
ORDER BY
  payout_date ASC,
  crime_id ASC
LIMIT 100 