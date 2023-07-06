{% macro declare_underwriter_udfs() %}

    create or replace function
        {{ target.schema }}.calculate_policy_has_co_pay(pet_age_months int64)
    returns bool
    as (pet_age_months > (9 * 12))
    ;

    create or replace function
        {{ target.schema }}.calculate_claim_excess_amount(
            claim_is_continuation bool,
            product_excess float64,
            claim_invoice_amount float64
        )
    returns float64
    as
        (
            case
                when claim_is_continuation is null
                then null
                when claim_is_continuation = true
                then 0
                when claim_is_continuation = false
                then least(product_excess, claim_invoice_amount)
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_claim_reserve_amount(
            claim_invoice_amount float64,
            claim_status string,
            claim_is_continuation bool,
            product_excess float64,
            claim_co_pay_percent int64,
            product_cover float64,
            policy_paid float64
        )
    returns float64
    as
        (
            case
                when claim_invoice_amount is null
                then 1
                when claim_status in ('declined', 'accepted', 'abeyance')
                then 0
                else
                    least(
                        (
                            claim_invoice_amount
                            - {{ target.schema }}.calculate_claim_excess_amount(
                                claim_is_continuation,
                                product_excess,
                                claim_invoice_amount
                            )
                        )
                        * ((100 - claim_co_pay_percent) / 100),
                        product_cover - policy_paid
                    )
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_claim_incurred_amount(
            claim_reserve_amount float64,
            claim_paid_amount float64,
            claim_recovery_amount float64
        )
    returns float64
    as
        (
            case
                when claim_paid_amount is null
                then claim_reserve_amount - coalesce(claim_recovery_amount, 0)
                else claim_paid_amount - coalesce(claim_recovery_amount, 0)
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_policy_exposure(
            policy_start_date date, snapshot_date date
        )
    returns float64
    as
        (
            least(
                date_diff(
                    greatest(snapshot_date, policy_start_date), policy_start_date, day
                ),
                366
            )
            / 366
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_policy_development_month(
            policy_start_date date, policy_end_date date, snapshot_date date
        )
    returns float64
    as
        (
            case
                when snapshot_date < policy_start_date
                then null
                else
                    date_diff(
                        least(snapshot_date, policy_end_date), policy_start_date, month
                    )
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_postcode_area(postal_code string)
    returns string
    as
        (
            case
                when regexp_contains(postal_code, r"^[A-Z][A-Z]")
                then substr(postal_code, 1, 2)
                when regexp_contains(postal_code, r"^[A-Z][0-9]")
                then substr(postal_code, 1, 1)
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_age_in_months(date_of_birth date, date_at date)
    returns int64
    as
        (
            case
                when date_at < date_of_birth
                then 0
                when extract(day from date_at) >= extract(day from date_of_birth)
                then date_diff(date_at, date_of_birth, month)
                else date_diff(date_at, date_of_birth, month) - 1
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_age_in_years(date_of_birth date, date_at date)
    returns int64
    as
        (
            cast(
                floor(
                    {{ target.schema }}.calculate_age_in_months(date_of_birth, date_at)
                    / 12
                ) as int64
            )
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_gross_written_premium(
            policy_premium_price float64,
            policy_start_date date,
            policy_cancel_date date
        )
    returns float64
    as
        (
            case
                when policy_cancel_date is null
                then policy_premium_price / 1.12
                when date_diff(policy_cancel_date, policy_start_date, day) <= 14
                then 0
                else
                    (
                        policy_premium_price / 1.12
                    ) * {{ target.schema }}.calculate_policy_exposure(
                        policy_start_date, policy_cancel_date
                    )
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_gross_earned_premium(
            policy_premium_price float64,
            policy_start_date date,
            policy_cancel_date date,
            snapshot_date date
        )
    returns float64
    as
        (
            case
                when policy_cancel_date is null
                then
                    (
                        policy_premium_price / 1.12
                    ) * {{ target.schema }}.calculate_policy_exposure(
                        policy_start_date, snapshot_date
                    )
                when date_diff(policy_cancel_date, policy_start_date, day) <= 14
                then 0
                else
                    (
                        policy_premium_price / 1.12
                    ) * {{ target.schema }}.calculate_policy_exposure(
                        policy_start_date, least(snapshot_date, policy_cancel_date)
                    )
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_premium_price(
            retail_price float64, discount_percentage float64
        )
    returns float64
    as
        (
            case
                when discount_percentage is not null
                then retail_price / (1 - discount_percentage / 100)
                else retail_price
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_retail_price(
            premium_price float64, discount_percentage float64
        )
    returns float64
    as
        (
            case
                when discount_percentage is not null
                then premium_price * (1 - discount_percentage / 100)
                else premium_price
            end
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_consumed_amount(
            amount float64, start_date date, end_date date, cancel_date date
        )
    returns float64
    as
        (
            round(
                cast(
                    case
                        when date_diff(end_date, start_date, day) > 0
                        then
                            greatest(
                                amount
                                * (date_diff(cancel_date, start_date, day) / 365),
                                0
                            )
                        else 0.0
                    end as numeric
                ),
                2,
                "ROUND_HALF_EVEN"
            )
        )
    ;

    create or replace function
        {{ target.schema }}.calculate_amount_exc_ipt(amount float64)
    returns float64
    as (amount / (1 + 12 / 100))
    ;

{% endmacro %}
