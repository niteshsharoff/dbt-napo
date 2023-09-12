with
    referrers as (
        select distinct
            referral_code,
            user_id as referrer_user_id,
            first_name as referrer_first_name,
            last_name as referrer_last_name,
            email as referrer_email
        from raw.customer a
        left join raw.user b on a.user_id = b.id
        where referral_code is not null
    ),

    referrals_cleaned as (
        select distinct
            a.id,
            a.referral_code,
            a.referral_policy_id as referee_policy_id,
            a.run_date,
            a.reward_type,
            c.email as referee_email,
            b.user_id as referee_user_id,
            first_name as referee_first_name,
            last_name as referee_last_name
        from raw.activatedreferral a
        left join raw.customer b on a.referee_id = b.customer_id
        left join raw.user c on b.user_id = c.id
        where reward_type != 'firstvet'
    ),

    final_table as (
        select
            a.id,
            a.referral_code,
            a.reward_type,
            referrer_user_id,
            referrer_first_name,
            referrer_last_name,
            referrer_email,
            referee_user_id,
            referee_policy_id,
            referee_first_name,
            referee_last_name,
            referee_email,
            run_date,
        from referrals_cleaned a
        left join referrers b on a.referral_code = b.referral_code
    )

select *
from final_table
