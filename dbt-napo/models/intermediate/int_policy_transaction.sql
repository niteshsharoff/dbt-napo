{% set MTA_FIELDS = [
    ["policy", "policy_id"],
    ["policy", "reference_number"],
    ["policy", "quote_id"],
    ["policy", "quote_source"],
    ["policy", "original_quote_source"],
    ["policy", "annual_price"],
    ["policy", "payment_plan_type"],
    ["policy", "start_date"],
    ["policy", "end_date"],
    ["policy", "policy_year"],
    ["policy", "cancel_date"],
    ["policy", "cancel_reason"],
    ["customer", "customer_id"],
    ["customer", "first_name"],
    ["customer", "last_name"],
    ["customer", "date_of_birth"],
    ["customer", "postal_code"],
    ["pet", "pet_id"],
    ["pet", "name"],
    ["pet", "date_of_birth"],
    ["pet", "gender"],
    ["pet", "cost"],
    ["pet", "is_neutered"],
    ["pet", "is_microchipped"],
    ["pet", "is_vaccinated"],
    ["pet", "species"],
    ["pet", "breed_category"],
    ["pet", "breed_name"],
    ["pet", "has_pre_existing_conditions"],
    ["product", "reference"],
] %}

with
    policy_history as (
        select
            row_effective_from,
            row_effective_to,
            struct(
                quote.quote_id,
                quote.pricing_model_version,
                quote.msm_sales_tracking_urn,
                timestamp_millis(quote.created_at) as created_at
            ) as quote,
            struct(
                policy.policy_id,
                policy.quote_id,
                policy.product_id,
                policy.customer_id,
                policy.pet_id,
                policy.voucher_code_id as voucher_id,
                policy.original_policy_id,
                policy.uuid,
                policy.reference_number,
                policy.quote_source,
                policy.original_quote_source,
                ifnull(policy.current_policy_year, 0) as policy_year,
                policy.payment_plan_type,
                policy.annual_payment_id,
                cast(policy.annual_price as numeric) as annual_price,
                policy.notes,
                policy.accident_cover_start_date,
                policy.illness_cover_start_date,
                policy.created_date,
                policy.start_date,
                policy.end_date,
                policy.cancel_date,
                policy.cancel_reason_id,
                policy.cancel_reason,
                policy.cancel_detail,
                policy.sold_at,
                policy.cancelled_at,
                policy.reinstated_at,
                policy.change_reason,
                policy.effective_from,
                policy.effective_to
            ) as policy,
            struct(
                customer.customer_id,
                customer.uuid,
                customer.first_name,
                customer.last_name,
                customer.email,
                customer.street_address,
                customer.address_locality,
                customer.address_region,
                customer.postal_code,
                customer.date_of_birth,
                customer.change_reason,
                customer.effective_from,
                customer.effective_to
            ) as customer,
            struct(
                pet.pet_id,
                pet.uuid,
                pet.name,
                pet.date_of_birth,
                case
                    when pet.gender = '1'
                    then 'male'
                    when pet.gender = '2'
                    then 'female'
                    else null
                end as gender,
                pet.size,
                pet.cost,
                pet.is_neutered,
                pet.is_microchipped,
                pet.is_vaccinated,
                pet.species,
                pet.breed_category,
                pet.breed_name,
                pet.breed_source,
                pet.has_pre_existing_conditions,
                pet.change_reason,
                pet.multipet_number,
                pet.effective_from,
                pet.effective_to
            ) as pet,
            struct(
                product.id as product_id,
                product.reference,
                product.name,
                product.vet_fee_cover,
                product.complementary_treatment_cover,
                product.dental_cover,
                product.emergency_boarding_cover,
                product.third_person_liability_excess,
                product.third_person_liability_cover,
                product.pet_death_cover,
                product.travel_cover,
                product.missing_pet_cover,
                product.behavioural_treatment_cover,
                product.co_pay,
                product.excess
            ) as product,
            struct(
                campaign.voucher_id,
                campaign.voucher_code,
                cast(campaign.discount_percentage as numeric) as discount_percentage,
                campaign.affiliate_channel
            ) as campaign
        from {{ ref("int_policy_history") }}
    ),
    policy_history_with_audit_dimension as (
        select
            quote,
            (
                select as struct
                    policy.* except (change_reason, effective_from, effective_to)
            ) as policy,
            (
                select as struct
                    customer.* except (change_reason, effective_from, effective_to)
            ) as customer,
            (
                select as struct
                    pet.* except (change_reason, effective_from, effective_to)
            ) as pet,
            product,
            campaign,
            struct(
                {% for mta_field in MTA_FIELDS -%}
                    {% set model = mta_field[0] -%}
                    {% set column = mta_field[1] -%}
                    case
                        when
                            {{ model }}.{{ column }}
                            != lag({{ model }}.{{ column }}) over (
                                partition by policy.policy_id
                                order by row_effective_from
                            )
                            or (
                                {{ model }}.{{ column }} is not null
                                and lag({{ model }}.{{ column }}) over (
                                    partition by policy.policy_id
                                    order by row_effective_from
                                )
                                is null
                            )
                        then true
                        else false
                    end as {{ model }}_{{ column }}_changed
                    {%- if not loop.last %}, {% endif %}
                {% endfor %},
                policy.change_reason as policy_row_change_reason,
                customer.change_reason as customer_row_change_reason,
                pet.change_reason as pet_row_change_reason,
                policy.effective_from as policy_row_effective_from,
                policy.effective_to as policy_row_effective_to,
                customer.effective_from as customer_row_effective_from,
                customer.effective_to as customer_row_effective_to,
                pet.effective_from as pet_row_effective_from,
                pet.effective_to as pet_row_effective_to
            ) as _audit,
            row_effective_from,
            row_effective_to
        from policy_history
    ),
    new_policies as (
        select 'New Policy' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension p
        where
            policy.quote_source != 'renewal'
            and row_effective_from = (
                select min(policy.sold_at)
                from policy_history_with_audit_dimension
                where policy.policy_id = p.policy.policy_id
            )
    ),
    ntus as (
        select 'NTU' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension
        where
            row_effective_from = policy.cancelled_at
            and policy.reinstated_at is null
            and date_diff(policy.cancel_date, policy.start_date, day) < 14
    ),
    new_cancellations as (
        select
            'Cancellation' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension
        where
            row_effective_from = policy.cancelled_at
            and policy.reinstated_at is null
            and date_diff(policy.cancel_date, policy.start_date, day) >= 14
    ),
    renewals as (
        select 'Renewal' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension r
        where
            policy.quote_source = 'renewal'
            and row_effective_from = (
                select min(policy.sold_at)
                from policy_history_with_audit_dimension
                where policy.policy_id = r.policy.policy_id
            )
    ),
    reinstatements as (
        select
            'Reinstatement' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension
        where row_effective_from = policy.reinstated_at
    ),
    reinstated_ntus as (
        -- Handles Reinstated NTUs (i.e. ADV-MUR-0021)
        select 'NTU' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension
        where
            row_effective_from = policy.cancelled_at
            and policy.reinstated_at is not null
            and date_diff(policy.cancel_date, policy.start_date, day) < 14
    ),
    reinstated_cancellations as (
        select
            'Cancellation' as transaction_type, row_effective_from as transaction_at, *
        from policy_history_with_audit_dimension
        where
            row_effective_from = policy.cancelled_at
            and policy.reinstated_at is not null
            and date_diff(policy.cancel_date, policy.start_date, day) >= 14
    ),
    all_mtas as (
        select *
        from policy_history_with_audit_dimension
        where
            (
                row_effective_from != policy.sold_at
                and (
                    row_effective_from != policy.cancelled_at
                    or policy.cancelled_at is null
                )
                and (
                    row_effective_from != policy.reinstated_at
                    or policy.reinstated_at is null
                )
                and (
                    {% for mta_field in MTA_FIELDS -%}
                        {% set model = mta_field[0] -%}
                        {% set column = mta_field[1] -%}
                        _audit.{{ model }}_{{ column }}_changed
                        {%- if not loop.last %} or{% endif %}
                    {% endfor %}
                )
            )
    ),
    sold_mtas as (
        select 'MTA' as transaction_type, row_effective_from as transaction_at, *
        from all_mtas
        where
            policy.cancelled_at is null or (policy.reinstated_at > policy.cancelled_at)
    ),
    ntu_mtas as (
        -- Handles MTA on NTU policies (i.e. ADV-CIR-0002)
        select 'NTU' as transaction_type, row_effective_from as transaction_at, *
        from all_mtas
        where
            policy.cancelled_at is not null
            and (
                (policy.cancelled_at > policy.reinstated_at)
                or (policy.reinstated_at is null)
            )
            and date_diff(policy.cancel_date, policy.start_date, day) < 14
    ),
    cancellation_mtas as (
        select
            'Cancellation MTA' as transaction_type,
            row_effective_from as transaction_at,
            *
        from all_mtas
        where
            policy.cancelled_at is not null
            and (
                (policy.cancelled_at > policy.reinstated_at)
                or (policy.reinstated_at is null)
            )
            and date_diff(policy.cancel_date, policy.start_date, day) >= 14
    ),
    all_transactions as (
        select *
        from new_policies
        union all
        select *
        from ntus
        union all
        select *
        from new_cancellations
        union all
        select *
        from sold_mtas
        union all
        select *
        from renewals
        union all
        select *
        from reinstatements
        union all
        select *
        from reinstated_ntus
        union all
        select *
        from reinstated_cancellations
        union all
        select *
        from ntu_mtas
        union all
        select *
        from cancellation_mtas
    ),
    final as (
        select
            transaction_type,
            transaction_at,
            quote,
            (
                select as struct
                    policy.* except (
                        quote_id, product_id, customer_id, pet_id, voucher_id
                    )
            ) as policy,
            customer,
            pet,
            product,
            campaign,
            _audit
        from all_transactions
        order by transaction_at
    )
select *
from final
