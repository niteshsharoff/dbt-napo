{{ config(materialized="incremental", unique_key="date", schema="marts") }}

with
    preliminary_metrics as (
        select
            date(current_date()) as date,
            format_date('%V', current_date()) as week,
            format_date('%A', current_date()) as weekday,
            countif(
                pet_current_age_months <= 5 and species = 'dog'
            ) as active_puppy_policies_dog_only,
            countif(
                pet_current_age_months >= 6
                and pet_current_age_months <= 24
                and species = 'dog'
            ) as active_adolescent_policies_dog_only
        from {{ ref("dim_policy_detail") }}
        where
            is_policy_expired is false
            and (is_subscription_active is not null or annual_payment_id is not null)
            and policy_cancel_date is null
    ),

    viewer_details as (
        select distinct customer_id, count(*) as num_views
        from
            (
                select distinct
                    date(time) as date_viewed, a.user_id, identity, uuid, customer_id
                from heap.training_socialisation_view_puppy_academy_dashboard a

                left join heap.users b on a.user_id = b.user_id
                left join raw.customer c on b.identity = c.uuid
            )
        where uuid is not null
        group by 1
    ),

    pa_eligible_customers as (
        select distinct customer_id, format_date('%Y-%m', created_date) as cohort,
        from dbt.dim_policy_detail
        where
            (is_subscription_active is not null or annual_payment_id is not null)
            and policy_cancel_date is null
            and species = "dog"
            and pet_current_age_months <= 5
            and is_policy_expired = false
    ),

    pa_eligible_details as (
        select a.customer_id, cohort, num_views
        from pa_eligible_customers a
        left join viewer_details b on a.customer_id = b.customer_id
    ),

    overall_pa_eligible_metrics as (
        select
            current_date() as date,
            countif(num_views > 0) as pa_eligible_viewed_dashboard,
            count(customer_id) as pa_eligible
        from pa_eligible_details
    ),

    newly_pa_eligible_metrics as (
        select
            current_date() as date,
            cohort as newly_pa_eligible_cohort,
            countif(num_views > 0) as newly_pa_eligible_viewed_dashboard,
            count(distinct customer_id) as newly_pa_dashboard
        from pa_eligible_details
        group by 1, 2
        order by 2 desc
        limit 1
    )

select
    a.date,
    week,
    weekday,
    active_puppy_policies_dog_only,
    active_adolescent_policies_dog_only,
    pa_eligible_viewed_dashboard,
    pa_eligible,
    newly_pa_eligible_cohort,
    newly_pa_eligible_viewed_dashboard,
    newly_pa_dashboard
from preliminary_metrics a
left join overall_pa_eligible_metrics b on a.date = b.date
left join newly_pa_eligible_metrics c on a.date = c.date
