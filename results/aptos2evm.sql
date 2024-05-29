SELECT DISTINCT
  ut.hash as hash,
  ut.sender as user_address,
  FIRST_VALUE(CASE WHEN REGEXP_LIKE(t.argument, '^"\d+"$') THEN TRIM(t.argument, '"') END) OVER (PARTITION BY ut.hash ORDER BY t.index) AS destination_chain_id,
  MAX(
    CASE
      WHEN REGEXP_LIKE(t.argument, '^"0x000000000000000000000000[0-9a-fA-F]{40}"')
      THEN CONCAT(
        CAST(COALESCE(CAST(COALESCE(TRY_CAST(TRY_CAST(0x AS VARCHAR) AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR),
        CAST(COALESCE(
          CAST(COALESCE(
            TRY_CAST(SUBSTR(TRIM(t.argument, '"'), 27, 40) AS VARCHAR),
            ''
          ) AS VARCHAR),
          ''
        ) AS VARCHAR)
      )
    END
  ) OVER (PARTITION BY ut.hash) AS _to
FROM aptos.user_transactions AS ut
CROSS JOIN UNNEST(ut.arguments) WITH ORDINALITY AS t(argument, index)
WHERE
  ut.entry_function_module_address = FROM_HEX('f22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa')
  AND ut.timestamp <= TRY_CAST(TO_UNIXTIME(TRY_CAST('2024-05-02 00:00:00' AS TIMESTAMP)) AS BIGINT) * 1000000
  AND ut.success = TRUE
  AND ut.sender <> FROM_HEX('1d8727df513fa2a8785d0834e40b34223daff1affc079574082baadb74b66ee4')
order by block_number
