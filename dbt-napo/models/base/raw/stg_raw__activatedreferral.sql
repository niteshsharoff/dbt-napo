with 

source as (

    select * from {{ source('raw', 'activatedreferral') }}

),

renamed as (

    select
        id,
        referral_code,
        email,
        created_at,
        used_at,
        referee_id,
        referral_policy_id,
        rewarded,
        validate_at,
        referee_reward,
        referrer_reward,
        referee_reward_mailchimp_email_id,
        referrer_reward_mailchimp_email_id,
        reward_type,
        uuid,
        referee_wegift_order_id,
        referrer_wegift_order_id,
        run_date

    from source

)

select * from renamed
