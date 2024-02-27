with
    temporal_join as (
        select
            claim.claim_uuid,
            claim.claim_reference,
            claim.master_claim_reference,
            claim.status,
            claim.sub_type,
            claim.cover_type,
            claim.submitted_at,
            claim.first_invoice_date,
            claim.last_invoice_date,
            claim.closed_date,
            claim.recovery_amount_mu,
            claim.decision,
            claim.decline_reason,
            condition.condition,
            condition.onset_date,
            condition.is_continuation,
            payment.invoice_amount_mu,
            payment.paid_amount_mu,
            claim.policy_id,
            claim.product_id,
            claim.pet_id,
            claim.customer_id,
            greatest(
                greatest(claim.effective_from, condition.effective_from),
                payment.effective_from
            ) as effective_from,
            least(
                least(claim.effective_to, condition.effective_to), payment.effective_to
            ) as effective_to
        from {{ ref("stg_claim_service__claim") }} claim
        join
            {{ ref("stg_claim_service__condition") }} condition
            on claim.claim_uuid = condition.claim_uuid
            and claim.effective_from <= condition.effective_to
            and claim.effective_to >= condition.effective_from
        join
            {{ ref("stg_claim_service__payment_breakdown") }} payment
            on claim.claim_uuid = payment.claim_uuid
            and claim.effective_from <= payment.effective_to
            and claim.effective_to >= payment.effective_from
            and condition.effective_from <= payment.effective_to
            and condition.effective_to >= payment.effective_from
    )
select *
from temporal_join
