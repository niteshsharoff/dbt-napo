select pk
      ,fields.policyid as policy_id
      ,fields.referencenumber as reference_number
      ,fields.* except(policyid, referencenumber)
from {{source('postgres','policy')}}