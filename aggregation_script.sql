"-- Set the ETL date (typically the previous day)
SET etl_date = '2023-10-15'::DATE;  -- Replace with current_date - 1 in production

-- Create Main DAU table

CREATE TEMP TABLE main AS
SELECT timestamp::DATE, user_id
FROM jigma.dau
WHERE timestamp::DATE = etl_date;

-- Create temporary table for user level from sr_action
CREATE TEMP TABLE user_level AS
SELECT
    ul.user_id,
    CAST(ul.description AS INTEGER) AS player_level_end_of_day
FROM (
    SELECT
        a.user_id,
        a.description,
        ROW_NUMBER() OVER (
            PARTITION BY a.user_id
            ORDER BY a.timestamp DESC
        ) AS rn
    FROM jigma.sr_action a
    WHERE a.action_type_id = 'levelup'
      AND a.timestamp::DATE <= etl_date
) ul
WHERE ul.rn = 1;

-- Create temporary table for experience points from sr_economy
CREATE TEMP TABLE experience_points AS
SELECT
    ep.user_id,
    ep.balance_after AS experience_points_end_of_day,
    ep.balance_before AS experience_points_start_of_day
FROM (
    SELECT
        e.user_id,
        e.balance_after,
        e.balance_before,
        ROW_NUMBER() OVER (
            PARTITION BY e.user_id
            ORDER BY e.timestamp DESC
        ) AS rn
    FROM jigma.sr_economy e
    WHERE e.item_type = 'experience_points'
      AND e.timestamp::DATE <= etl_date
) ep
WHERE ep.rn = 1;

-- Create temporary table for currency balances from sr_economy
CREATE TEMP TABLE latest_currency_balances AS
SELECT
    e.user_id,
    e.item_type,
    e.balance_after,
    ROW_NUMBER() OVER (
        PARTITION BY e.user_id, e.item_type
        ORDER BY e.timestamp DESC
    ) AS rn
FROM jigma.sr_economy e
WHERE e.item_type IN ('gem', 'gold', 'trophies')
  AND e.timestamp::DATE <= etl_date;

CREATE TEMP TABLE currency_balances AS
SELECT
    users.user_id,
    gem.balance_after AS gem_wallet_end_of_day,
    gold.balance_after AS gold_wallet_end_of_day,
    trophies.balance_after AS trophies_end_of_day
FROM (SELECT DISTINCT user_id FROM latest_currency_balances) users
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_currency_balances
    WHERE item_type = 'gem' AND rn = 1
) gem ON users.user_id = gem.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_currency_balances
    WHERE item_type = 'gold' AND rn = 1
) gold ON users.user_id = gold.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_currency_balances
    WHERE item_type = 'trophies' AND rn = 1
) trophies ON users.user_id = trophies.user_id;

-- Create temporary table for card levels from sr_card_upgrades
CREATE TEMP TABLE card_levels AS
SELECT
    clu.user_id,
    MAX(CASE WHEN clu.card_id = 'golem' THEN clu.new_level END) AS golem_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'pekka' THEN clu.new_level END) AS pekka_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'princess' THEN clu.new_level END) AS princess_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'wizard' THEN clu.new_level END) AS wizard_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'archer' THEN clu.new_level END) AS archer_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'knight' THEN clu.new_level END) AS knight_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'giant' THEN clu.new_level END) AS giant_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'minion' THEN clu.new_level END) AS minion_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'hog_rider' THEN clu.new_level END) AS hog_rider_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'baby_dragon' THEN clu.new_level END) AS baby_dragon_level_end_of_day
FROM (
    SELECT
        cu.user_id,
        cu.card_id,
        cu.new_level,
        ROW_NUMBER() OVER (
            PARTITION BY cu.user_id, cu.card_id
            ORDER BY cu.timestamp DESC
        ) AS rn
    FROM jigma.sr_card_upgrades cu
    WHERE cu.timestamp::DATE <= etl_date
) clu
WHERE clu.rn = 1
GROUP BY clu.user_id;

-- Create temporary table for latest card balances from sr_economy
CREATE TEMP TABLE latest_card_balances AS
SELECT
    e.user_id,
    e.item_type,
    e.balance_after,
    ROW_NUMBER() OVER (
        PARTITION BY e.user_id, e.item_type
        ORDER BY e.timestamp DESC
    ) AS rn
FROM jigma.sr_economy e
WHERE e.item_type IN (
    'golem_card', 'pekka_card', 'princess_card', 'wizard_card', 'archer_card',
    'knight_card', 'giant_card', 'minion_card', 'hog_rider_card', 'baby_dragon_card'
  )
  AND e.timestamp::DATE <= etl_date;

-- Create temporary table for card counts
CREATE TEMP TABLE card_counts AS
SELECT
    users.user_id,
    golem.balance_after AS golem_cards_owned_end_of_day,
    pekka.balance_after AS pekka_cards_owned_end_of_day,
    princess.balance_after AS princess_cards_owned_end_of_day,
    wizard.balance_after AS wizard_cards_owned_end_of_day,
    archer.balance_after AS archer_cards_owned_end_of_day,
    knight.balance_after AS knight_cards_owned_end_of_day,
    giant.balance_after AS giant_cards_owned_end_of_day,
    minion.balance_after AS minion_cards_owned_end_of_day,
    hog_rider.balance_after AS hog_rider_cards_owned_end_of_day,
    baby_dragon.balance_after AS baby_dragon_cards_owned_end_of_day
FROM (SELECT DISTINCT user_id FROM latest_card_balances) users
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'golem_card' AND rn = 1
) golem ON users.user_id = golem.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'pekka_card' AND rn = 1
) pekka ON users.user_id = pekka.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'princess_card' AND rn = 1
) princess ON users.user_id = princess.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'wizard_card' AND rn = 1
) wizard ON users.user_id = wizard.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'archer_card' AND rn = 1
) archer ON users.user_id = archer.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'knight_card' AND rn = 1
) knight ON users.user_id = knight.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'giant_card' AND rn = 1
) giant ON users.user_id = giant.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'minion_card' AND rn = 1
) minion ON users.user_id = minion.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'hog_rider_card' AND rn = 1
) hog_rider ON users.user_id = hog_rider.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'baby_dragon_card' AND rn = 1
) baby_dragon ON users.user_id = baby_dragon.user_id;

-- Create temporary table for gem flows from sr_economy
CREATE TEMP TABLE gem_flows AS
SELECT
    e.user_id,
    SUM(CASE WHEN e.transaction_flow_type = 'inflow' AND e.item_type = 'gem' THEN e.amount ELSE 0 END) AS gem_inflow_total,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' THEN e.amount ELSE 0 END) AS gem_outflow_total,
    -- Specific gem outflows for creature purchases
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'golem_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_golem,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'pekka_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_pekka,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'princess_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_princess,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'wizard_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_wizard,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'archer_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_archer,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'knight_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_knight,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'giant_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_giant,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'minion_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_minion,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'hog_rider_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_hog_rider,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'baby_dragon_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_baby_dragon
FROM jigma.sr_economy e
WHERE e.timestamp::DATE = etl_date
GROUP BY e.user_id;

-- Create temporary table for gold flows from sr_economy
CREATE TEMP TABLE gold_flows AS
SELECT
    e.user_id,
    SUM(CASE WHEN e.transaction_flow_type = 'inflow' AND e.item_type = 'gold' THEN e.amount ELSE 0 END) AS gold_inflow_total,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' THEN e.amount ELSE 0 END) AS gold_outflow_total,
    -- Specific gold outflows for creature upgrades
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'golem_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_golem_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'pekka_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_pekka_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'princess_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_princess_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'wizard_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_wizard_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND