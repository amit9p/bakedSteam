
SELECT 
    instnc_id,
    customer_id,
    snap_dt,
    first_name,
    last_name,
    middle_name
FROM CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_CUSTOMER_OS
WHERE 
       REGEXP_LIKE(first_name,  '.*[^[:ascii:]].*')
    OR REGEXP_LIKE(last_name,   '.*[^[:ascii:]].*')
    OR REGEXP_LIKE(middle_name, '.*[^[:ascii:]].*')
LIMIT 100;
<><><><>

SELECT 
    instnc_id,
    customer_id,
    snap_dt,
    first_name,
    last_name,
    middle_name,
    -- flag which specific field has non-ASCII
    CASE WHEN REGEXP_LIKE(first_name,  '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS first_name_has_non_ascii,
    CASE WHEN REGEXP_LIKE(last_name,   '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS last_name_has_non_ascii,
    CASE WHEN REGEXP_LIKE(middle_name, '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS middle_name_has_non_ascii
FROM CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_CUSTOMER_OS
WHERE snap_dt = '2026-06-02'  -- adjust to your target date
  AND (
        REGEXP_LIKE(first_name,  '[^\\x00-\\x7F]')
     OR REGEXP_LIKE(last_name,   '[^\\x00-\\x7F]')
     OR REGEXP_LIKE(middle_name, '[^\\x00-\\x7F]')
  )
ORDER BY instnc_id;
------------

WITH cust AS (
    SELECT 
        instnc_id   AS customer_instnc_id,
        customer_id,
        snap_dt,
        first_name,
        last_name,
        middle_name,
        CASE WHEN REGEXP_LIKE(first_name,  '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS first_name_non_ascii,
        CASE WHEN REGEXP_LIKE(last_name,   '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS last_name_non_ascii,
        CASE WHEN REGEXP_LIKE(middle_name, '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS middle_name_non_ascii
    FROM CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_CUSTOMER_OS
    WHERE snap_dt = '2026-06-02'
),
addr AS (
    SELECT 
        instnc_id   AS address_instnc_id,
        address_id,
        customer_id,
        snap_dt,
        address_line_1,
        address_line_2,
        city,
        CASE WHEN REGEXP_LIKE(address_line_1, '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS addr1_non_ascii,
        CASE WHEN REGEXP_LIKE(address_line_2, '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS addr2_non_ascii,
        CASE WHEN REGEXP_LIKE(city,           '[^\\x00-\\x7F]') THEN 'Y' ELSE 'N' END AS city_non_ascii
    FROM CARD_DB.QHDP_CARD.RCVRY_ACCT_SRVC_ADDRESS_OS
    WHERE snap_dt = '2026-06-02'
)
SELECT 
    c.customer_instnc_id,
    a.address_instnc_id,
    c.customer_id,
    c.snap_dt,
    -- customer fields
    c.first_name,
    c.last_name,
    c.middle_name,
    c.first_name_non_ascii,
    c.last_name_non_ascii,
    c.middle_name_non_ascii,
    -- address fields
    a.address_id,
    a.address_line_1,
    a.address_line_2,
    a.city,
    a.addr1_non_ascii,
    a.addr2_non_ascii,
    a.city_non_ascii
FROM cust c
FULL OUTER JOIN addr a
    ON c.customer_id = a.customer_id
   AND c.snap_dt    = a.snap_dt
WHERE 
    c.first_name_non_ascii = 'Y' OR c.last_name_non_ascii = 'Y' OR c.middle_name_non_ascii = 'Y'
 OR a.addr1_non_ascii      = 'Y' OR a.addr2_non_ascii     = 'Y' OR a.city_non_ascii        = 'Y'
ORDER BY c.customer_instnc_id, a.address_instnc_id;
