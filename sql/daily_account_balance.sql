SELECT
    account_id,
    txn_date,
    SUM(amount) AS total_amount,
    COUNT(*) AS txn_count
FROM ledger_transactions
GROUP BY
    account_id,
    txn_date
ORDER BY
    account_id,
    txn_date;
