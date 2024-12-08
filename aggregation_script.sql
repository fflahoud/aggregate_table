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
    ep.balance_after AS experience_points_end_of_day
FROM (
    SELECT
        e.user_id,
        e.balance_after,
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
    MAX(CASE WHEN clu.card_id = 'baby_dragon' THEN clu.new_level END) AS baby_dragon_level_end_of_day,
    MAX(CASE WHEN clu.card_id = 'golden_giant' THEN clu.new_level END) AS golden_giant_level_end_of_day
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
    'knight_card', 'giant_card', 'minion_card', 'hog_rider_card', 'baby_dragon_card', 'golden_giant_card'
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
    baby_dragon.balance_after AS baby_dragon_cards_owned_end_of_day,
    golden_giant.balance_after AS golden_giant_cards_owned_end_of_day
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
) baby_dragon ON users.user_id = baby_dragon.user_id
LEFT JOIN (
    SELECT user_id, balance_after
    FROM latest_card_balances
    WHERE item_type = 'golden_giant_card' AND rn = 1
) golden_giant ON users.user_id = golden_giant.user_id;

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
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'baby_dragon_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_baby_dragon,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gem' AND e.transaction_type_id = 'golden_giant_purchase' THEN e.amount ELSE 0 END) AS gem_outflow_golden_giant
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
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'archer_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_archer_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'knight_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_knight_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'giant_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_giant_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'minion_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_minion_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'hog_rider_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_hog_rider_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'baby_dragon_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_baby_dragon_upgrade,
    SUM(CASE WHEN e.transaction_flow_type = 'outflow' AND e.item_type = 'gold' AND e.transaction_type_id = 'golden_giant_upgrade' THEN e.amount ELSE 0 END) AS gold_outflow_golden_giant_upgrade
FROM jigma.sr_economy e
WHERE e.timestamp::DATE = etl_date
GROUP BY e.user_id;

-- Create temporary table for purchase metrics from sr_iap
CREATE TEMP TABLE purchase_metrics AS
SELECT
    iap.user_id,
    SUM(CASE WHEN iap.transaction_date <= etl_date THEN iap.amount_usd ELSE 0 END) AS lifetime_spend_usd,
    SUM(CASE WHEN iap.transaction_date BETWEEN etl_date - INTERVAL '6 days' AND etl_date THEN iap.amount_usd ELSE 0 END) AS spend_last_7_days_usd,
    SUM(CASE WHEN iap.transaction_date BETWEEN etl_date - INTERVAL '31 days' AND etl_date THEN iap.amount_usd ELSE 0 END) AS spend_last_32_days_usd,
    MAX(CASE WHEN iap.item_purchased = 'ads_free_package' AND iap.transaction_status = 'completed' THEN TRUE ELSE FALSE END) AS has_purchased_ads_free,
    MAX(CASE WHEN iap.item_purchased = 'battle_pass' AND iap.transaction_status = 'completed' THEN TRUE ELSE FALSE END) AS has_purchased_battle_pass
FROM jigma.sr_iap iap
WHERE iap.transaction_date <= etl_date
GROUP BY iap.user_id;

-- Create temporary table for session metrics from sr_action
CREATE TEMP TABLE session_metrics AS
SELECT
    a.user_id,
    COUNT(DISTINCT a.session_id) AS sessions_started_today,
    MIN(a.timestamp) AS first_login_time,
    MAX(a.timestamp) AS last_login_time
FROM jigma.sr_action a
WHERE a.timestamp::DATE = etl_date
GROUP BY a.user_id;

-- Create temporary table for today's IAP transactions from sr_iap
CREATE TEMP TABLE iap_today AS
SELECT
    iap.user_id,
    COUNT(*) AS iap_transactions_count_today,
    SUM(iap.amount_usd) AS iap_total_amount_usd_today
FROM jigma.sr_iap iap
WHERE iap.transaction_date = etl_date
  AND iap.transaction_status = 'completed'
GROUP BY iap.user_id;

-- Create temporary table for ads metrics from sr_ads
CREATE TEMP TABLE ads_metrics AS
SELECT
    ads.user_id,
    COUNT(CASE WHEN ads.action = 'watched' THEN 1 END) AS ads_watched_today,
    COUNT(CASE WHEN ads.action = 'skipped' THEN 1 END) AS ads_skipped_today,
    SUM(CASE WHEN ads.action = 'watched' THEN ads.revenue_generated_usd ELSE 0 END) AS ads_revenue_generated_usd_today
FROM jigma.sr_ads ads
WHERE ads.timestamp::DATE = etl_date
GROUP BY ads.user_id;

-- Create temporary table for social interaction metrics from sr_social
CREATE TEMP TABLE social_metrics AS
SELECT
    s.user_id,
    COUNT(CASE WHEN s.interaction_type = 'message_sent' THEN 1 END) AS messages_sent_today,
    COUNT(CASE WHEN s.interaction_type = 'friend_request_sent' THEN 1 END) AS friend_requests_sent_today
FROM jigma.sr_social s
WHERE s.timestamp::DATE = etl_date
GROUP BY s.user_id;

-- Create temporary table for lifetime metrics from sr_action
CREATE TEMP TABLE lifetime_metrics AS
SELECT
    a.user_id,
    COUNT(CASE WHEN a.action_type_id = 'battle_played' THEN 1 END) AS total_battles_played,
    COUNT(CASE WHEN a.action_type_id = 'battle_won' THEN 1 END) AS total_battles_won,
    COUNT(CASE WHEN a.action_type_id = 'three_crown_win' THEN 1 END) AS total_three_crown_wins,
    COUNT(CASE WHEN a.action_type_id = 'ads_watched' THEN 1 END) AS ads_watched_lifetime
FROM jigma.sr_action a
WHERE a.timestamp::DATE <= etl_date
GROUP BY a.user_id;

-- Final aggregation and insertion into sr_daily_user_activity
INSERT INTO jigma.sr_daily_user_activity (
    date,
    user_id,
    player_level_end_of_day,
    experience_points_end_of_day,
    gem_wallet_end_of_day,
    gold_wallet_end_of_day,
    trophies_end_of_day,
    -- Card Levels End of Day
    golem_level_end_of_day,
    pekka_level_end_of_day,
    princess_level_end_of_day,
    wizard_level_end_of_day,
    archer_level_end_of_day,
    knight_level_end_of_day,
    giant_level_end_of_day,
    minion_level_end_of_day,
    hog_rider_level_end_of_day,
    baby_dragon_level_end_of_day,
    golden_giant_level_end_of_day,
    -- Card Counts End of Day
    golem_cards_owned_end_of_day,
    pekka_cards_owned_end_of_day,
    princess_cards_owned_end_of_day,
    wizard_cards_owned_end_of_day,
    archer_cards_owned_end_of_day,
    knight_cards_owned_end_of_day,
    giant_cards_owned_end_of_day,
    minion_cards_owned_end_of_day,
    hog_rider_cards_owned_end_of_day,
    baby_dragon_cards_owned_end_of_day,
    golden_giant_cards_owned_end_of_day,
    -- Gem Outflows for Creature Purchases
    gem_outflow_golem,
    gem_outflow_pekka,
    gem_outflow_princess,
    gem_outflow_wizard,
    gem_outflow_archer,
    gem_outflow_knight,
    gem_outflow_giant,
    gem_outflow_minion,
    gem_outflow_hog_rider,
    gem_outflow_baby_dragon,
    gem_outflow_golden_giant,
    -- Gold Outflows for Creature Upgrades
    gold_outflow_golem_upgrade,
    gold_outflow_pekka_upgrade,
    gold_outflow_princess_upgrade,
    gold_outflow_wizard_upgrade,
    gold_outflow_archer_upgrade,
    gold_outflow_knight_upgrade,
    gold_outflow_giant_upgrade,
    gold_outflow_minion_upgrade,
    gold_outflow_hog_rider_upgrade,
    gold_outflow_baby_dragon_upgrade,
    gold_outflow_golden_giant_upgrade,
    -- Other metrics...
    lifetime_spend_usd,
    spend_last_7_days_usd,
    spend_last_32_days_usd,
    has_purchased_ads_free,
    has_purchased_battle_pass,
    sessions_started_today,
    first_login_time,
    last_login_time,
    iap_transactions_count_today,
    iap_total_amount_usd_today,
    ads_watched_today,
    ads_skipped_today,
    ads_revenue_generated_usd_today,
    messages_sent_today,
    friend_requests_sent_today
)
SELECT
    etl_date AS date,
    ul.user_id,
    ul.player_level_end_of_day,
    xp.experience_points_end_of_day,
    cb.gem_wallet_end_of_day,
    cb.gold_wallet_end_of_day,
    cb.trophies_end_of_day,
    -- Card Levels End of Day
    cl.golem_level_end_of_day,
    cl.pekka_level_end_of_day,
    cl.princess_level_end_of_day,
    cl.wizard_level_end_of_day,
    cl.archer_level_end_of_day,
    cl.knight_level_end_of_day,
    cl.giant_level_end_of_day,
    cl.minion_level_end_of_day,
    cl.hog_rider_level_end_of_day,
    cl.baby_dragon_level_end_of_day,
    cl.golden_giant_level_end_of_day,
    -- Card Counts End of Day
    cc.golem_cards_owned_end_of_day,
    cc.pekka_cards_owned_end_of_day,
    cc.princess_cards_owned_end_of_day,
    cc.wizard_cards_owned_end_of_day,
    cc.archer_cards_owned_end_of_day,
    cc.knight_cards_owned_end_of_day,
    cc.giant_cards_owned_end_of_day,
    cc.minion_cards_owned_end_of_day,
    cc.hog_rider_cards_owned_end_of_day,
    cc.baby_dragon_cards_owned_end_of_day,
    cc.golden_giant_cards_owned_end_of_day,
    -- Gem Outflows for Creature Purchases
    gemf.gem_outflow_golem,
    gemf.gem_outflow_pekka,
    gemf.gem_outflow_princess,
    gemf.gem_outflow_wizard,
    gemf.gem_outflow_archer,
    gemf.gem_outflow_knight,
    gemf.gem_outflow_giant,
    gemf.gem_outflow_minion,
    gemf.gem_outflow_hog_rider,
    gemf.gem_outflow_baby_dragon,
    gemf.gem_outflow_golden_giant,
    -- Gold Outflows for Creature Upgrades
    goldf.gold_outflow_golem_upgrade,
    goldf.gold_outflow_pekka_upgrade,
    goldf.gold_outflow_princess_upgrade,
    goldf.gold_outflow_wizard_upgrade,
    goldf.gold_outflow_archer_upgrade,
    goldf.gold_outflow_knight_upgrade,
    goldf.gold_outflow_giant_upgrade,
    goldf.gold_outflow_minion_upgrade,
    goldf.gold_outflow_hog_rider_upgrade,
    goldf.gold_outflow_baby_dragon_upgrade,
    goldf.gold_outflow_golden_giant_upgrade,
    -- Other metrics...
    pm.lifetime_spend_usd,
    pm.spend_last_7_days_usd,
    pm.spend_last_32_days_usd,
    pm.has_purchased_ads_free,
    pm.has_purchased_battle_pass,
    sm.sessions_started_today,
    sm.first_login_time,
    sm.last_login_time,
    iap.iap_transactions_count_today,
    iap.iap_total_amount_usd_today,
    am.ads_watched_today,
    am.ads_skipped_today,
    am.ads_revenue_generated_usd_today,
    so.messages_sent_today,
    so.friend_requests_sent_today
FROM main m
LEFT JOIN user_level ul ON m.user_id = ul.user_id
LEFT JOIN experience_points xp ON m.user_id = xp.user_id
LEFT JOIN currency_balances cb ON m.user_id = cb.user_id
LEFT JOIN card_levels cl ON m.user_id = cl.user_id
LEFT JOIN card_counts cc ON m.user_id = cc.user_id
LEFT JOIN gem_flows gemf ON m.user_id = gemf.user_id
LEFT JOIN gold_flows goldf ON m.user_id = goldf.user_id
LEFT JOIN purchase_metrics pm ON m.user_id = pm.user_id
LEFT JOIN session_metrics sm ON m.user_id = sm.user_id
LEFT JOIN iap_today iap ON m.user_id = iap.user_id
LEFT JOIN ads_metrics am ON m.user_id = am.user_id
LEFT JOIN social_metrics so ON m.user_id = so.user_id;
"