WITH send_filtered AS (
    SELECT 
        tx_hash, 
        user_address,
        102 AS source_chain_id -- 默认赋值110
    FROM 
        layerzero.send
    WHERE 
        destination_chain_id = 108 
        AND source_chain_id = 102
        AND block_time <= TIMESTAMP '2024-05-02 00:00:00'
),
transactions_processed AS (
    SELECT 
        hash,
        case 
            when varbinary_starts_with(data, 0xca23bb4c) then '0x' || SUBSTRING(cast(data as VARCHAR), 11, 64)
            when varbinary_starts_with(data, 0x76a9099a) then '0x' || SUBSTRING(cast(data as VARCHAR), 75, 64)
            else null -- 或者你可以选择其他的默认值
        end as _to
    FROM 
        bnb.transactions
),
joined_data AS (
    SELECT 
        s.tx_hash,
        s.user_address,
        s.source_chain_id,
        t._to
    FROM 
        send_filtered s
    JOIN 
        transactions_processed t
    ON 
        s.tx_hash = t.hash
)
SELECT 
    source_chain_id,
    tx_hash,
    user_address,
    _to
FROM 
    joined_data
WHERE 
    _to IS NOT NULL