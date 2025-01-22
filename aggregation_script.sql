-- SQL
-- Set the ETL date (typically the previous day)
SET etl_date = '2023-10-15'::DATE;  -- Replace with current_date - 1 in production

-- Create Main DAU table
CREATE TEMP TABLE main AS
SELECT timestamp::DATE, user_id
FROM jigma.dau
WHERE timestamp::DATE = etl_date;

[Rest of the script continues exactly as in the previous response...]