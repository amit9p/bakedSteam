SELECT DISTINCT c.instnc_id
FROM CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_CUSTOMER_OS c
JOIN CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_ADDRESS_OS a
  ON c.instnc_id = a.instnc_id
WHERE (
        REGEXP_LIKE(c.first_name,  '.*[^[:ascii:]].*')
     OR REGEXP_LIKE(c.last_name,   '.*[^[:ascii:]].*')
     OR REGEXP_LIKE(c.middle_name, '.*[^[:ascii:]].*')
      )
  AND (
        REGEXP_LIKE(a.address_line_1, '.*[^[:ascii:]].*')
     OR REGEXP_LIKE(a.address_line_2, '.*[^[:ascii:]].*')
     OR REGEXP_LIKE(a.city,           '.*[^[:ascii:]].*')
      );


SELECT instnc_id
FROM CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_CUSTOMER_OS
WHERE REGEXP_LIKE(first_name,  '.*[^[:ascii:]].*')
   OR REGEXP_LIKE(last_name,   '.*[^[:ascii:]].*')
   OR REGEXP_LIKE(middle_name, '.*[^[:ascii:]].*')

INTERSECT

SELECT instnc_id
FROM CARD_DB.QHDP_CARD_NPI.RCVRY_ACCT_SRVC_ADDRESS_OS
WHERE REGEXP_LIKE(address_line_1, '.*[^[:ascii:]].*')
   OR REGEXP_LIKE(address_line_2, '.*[^[:ascii:]].*')
   OR REGEXP_LIKE(city,           '.*[^[:ascii:]].*');
