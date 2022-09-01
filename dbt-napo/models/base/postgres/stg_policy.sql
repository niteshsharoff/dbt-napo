select pk
      ,fields.policyid as policy_id
      ,fields._referencenumber as reference_number
      ,nullif(fields.annual_payment_id,'') as annual_payment_id
      ,nullif(fields.quote_source_reference,'') as quote_source_reference
      ,fields.* except(_petid,policyid, _referencenumber,annual_payment_id,quote_source_reference)
      ,fields._petid as pet
from {{source('postgres','policy')}}