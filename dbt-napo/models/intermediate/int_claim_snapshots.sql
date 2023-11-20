with
    raw_claim_snapshot as (
        select
            * except (snapshot_date),
            '' as condition_venom_code,
            '' as excess,
            '' as claim_uuid,
            snapshot_date
        from raw.claims_snapshot_v1 v1
        union all
        select *
        from raw.claims_snapshot_v2 v2
    ),
    claim_snapshot as (
        select
            id,
            cast(policy_id as int64) as policy_id,
            trim(master_claim_id) as master_claim_id,
            extract(
                date from timestamp_millis(cast(date_received as int64))
            ) as date_received,
            extract(
                date from timestamp_millis(cast(onset_date as int64))
            ) as onset_date,
            cover_type,
            cover_sub_type,
            cast(paid_amount as float64) as paid_amount,
            extract(
                date from timestamp_millis(cast(first_invoice_date as int64))
            ) as first_invoice_date,
            decline_reason,
            cast(invoice_amount as float64) as invoice_amount,
            case
                when status in ('accepted', 'accepted claim')
                then 'accepted'
                when status in ('declined', 'declined claim')
                then 'declined'
                else status
            end as status,
            case
                when is_continuation = 'Yes'
                then true
                when is_continuation = 'No'
                then false
                else null
            end as is_continuation,
            condition,
            decision,
            reassessment_requested,
            emails_to_vets_or_customer,
            previous_vet_one_name,
            previous_vet_one_email,
            previous_vet_two_name,
            previous_vet_two_email,
            json_extract_scalar(tag) as tag,
            source,
            0 as recovery_amount,  # TODO: Extract from ClickUp custom fields
            is_archived,
            extract(
                date from timestamp_millis(cast(last_invoice_date as int64))
            ) as last_invoice_date,
            extract(
                date from timestamp_millis(cast(closed_date as int64))
            ) as closed_date,
            vet_practice_name,
            snapshot_date,
        from raw_claim_snapshot as claim
        left join unnest(json_extract_array(tags)) tag
        where not is_archived and policy_id is not null
    ),
    pivot_tag_to_id_grain as (
        select
            * except (
                life_threatening_vet,
                life_threatening_pet,
                urgent,
                vulnerable,
                breed_discrepancy,
                breed_difference,
                age_discrepancy,
                dob_discrepancy,
                previously_checked,
                contacting_ph,
                contacting_vet,
                -- re-ordering metadata columns
                source,
                is_archived,
                snapshot_date
            ),
            struct(
                (life_threatening_vet or life_threatening_pet) as life_threatening,
                urgent,
                vulnerable,
                (breed_discrepancy or breed_difference) as breed_discrepancy,
                age_discrepancy,
                dob_discrepancy,
                previously_checked,
                contacting_ph,
                contacting_vet,
                aggressive_pet,
                aggression
            ) as tags,
            source,
            is_archived,
            snapshot_date
        from
            claim_snapshot pivot (
                max(true) for tag in (
                    'life-threatening' as life_threatening_vet,  -- vet claims
                    'lifethreatening' as life_threatening_pet,  -- typeform claims
                    'urgent',
                    'vulnerable',
                    'breed discrepancy' as breed_discrepancy,  -- vet claims
                    'breed difference' as breed_difference,  -- typeform claims
                    'age discrepancy' as age_discrepancy,
                    'dob discrepancy' as dob_discrepancy,
                    'previously checked' as previously_checked,
                    'contacting ph' as contacting_ph,
                    'contacting vet' as contacting_vet,
                    'aggressive pet' as aggressive_pet,
                    'aggression' as aggression
                )
            )
    )
select *
from pivot_tag_to_id_grain
