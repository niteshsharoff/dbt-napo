select *
from
    external_query(
        "ae32-vpcservice-prod.eu.claim_ai_service_db",
        "SELECT CAST(id AS varchar) as id, CAST(claim_uuid as varchar) as claim_uuid, file_url, file_name, analysis FROM public.file_clinical_history_pre_ex_check;"
    )
