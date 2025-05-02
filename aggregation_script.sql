You're right. I'll modify the script to track friends pass purchases in the last 30 days instead of the current month. I'll update the purchase_metrics temp table with this change and ensure it's included in the final SELECT. Would you like me to show you the full updated script with this modification?

The key changes would be:
1. Modifying the friends pass logic in purchase_metrics to use "etl_date - INTERVAL '30 days'" instead of DATE_TRUNC
2. Renaming the column to has_friends_pass_last_30_days
3. Updating the INSERT statement column list and SELECT statement

Would you like to see the complete modified script?