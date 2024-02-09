{% macro claim_bdx_udfs() %}

    -- pet age
    create or replace function
        {{ target.schema }}.calculate_pet_age_months(
            date_of_birth date, policy_start_date date
        )
    returns int64
    as (date_diff(policy_start_date, date_of_birth, month))
    ;

    -- reserve amount
    create or replace function
        {{ target.schema }}.calculate_reserve_amount(
            claim_status string,
            decision string,
            invoice_amount numeric,
            paid_amount numeric,
            product_excess numeric,
            pet_age_months int64,
            is_continuation bool,
            copay_percent int64
        )
    returns numeric
    as
        (
            case
                when invoice_amount is null
                then 1
                when
                    claim_status in ('accepted', 'declined', 'abeyance', 'archived')
                    or decision is not null
                then 0
                when is_continuation is false
                then
                    cast(
                        greatest(
                            invoice_amount
                            - coalesce(paid_amount, 0.0)
                            - product_excess,
                            0.0
                        ) - (
                            case
                                when pet_age_months >= (12 * 9)
                                then
                                    greatest(
                                        invoice_amount
                                        - coalesce(paid_amount, 0.0)
                                        - product_excess,
                                        0.0
                                    )
                                    * (copay_percent / 100)
                                else 0
                            end
                        ) as numeric
                    )
            end
        )
    ;

    -- incurred amount
    create or replace function
        {{ target.schema }}.calculate_incurred_amount(
            reserve numeric,
            paid_amount numeric,
            fee_invoice numeric,
            recovery_amount numeric
        )
    returns numeric
    as
        (
            cast(
                reserve
                + greatest(paid_amount, 0.0)
                + fee_invoice
                - greatest(recovery_amount, 0.0) as numeric
            )
        )

{% endmacro %}
