/*
  GIVEN
    a table of policy transactions

  WHEN
    we query all transactions

  THEN
    we expect no discount amounts are reported to CGICE
*/
select * from {{ ref("cgice_cumulative_premium_bdx") }} where discount_amount <> '0.00'
