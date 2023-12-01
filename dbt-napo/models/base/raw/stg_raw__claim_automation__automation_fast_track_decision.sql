select *
from
    external_query(
        "ae32-vpcservice-dev.eu.claim_automation_service_db",
        "SELECT CAST(uuid AS varchar), CAST(claim_uuid AS varchar), is_valid, validation_errors, is_approved, reasons_rejected, created_at, CAST(fast_track_type AS varchar), raw_input FROM automation_fast_track_decision"
    )
