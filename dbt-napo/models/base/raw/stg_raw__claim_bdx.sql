select
    cast(
        case
            when source_name = 'Napo_Pet_Claim_Bdx_2021-11-01.csv'
            then '2021-12-01'
            when source_name = 'Napo_Pet_Claim_Bdx_2021-12-01.csv'
            then '2022-01-01'
            when source_name = 'Napo_Pet_Claim_Bdx_2022-01-01.csv'
            then '2022-02-01'
            when source_name = 'Napo_Pet_Claim_Bdx_2022-02-01.csv'
            then '2022-03-01'
            when source_name = 'Napo_Pet_Claim_Bdx_2022-03-01.csv'
            then '2022-04-01'
            when source_name = 'Napo_Pet_Claim_Bdx_2022-04-01.csv'
            then '2022-05-01'
            when source_name = 'napo_pet_claim_bdx_2022-05-01.csv'
            then '2022-06-01'
            when source_name = 'napo_pet_claim_bdx_2022-06-01.csv'
            then '2022-07-01'
            -- WHEN Source_Name = 'napo_pet_claim_bdx_2022-07-01.csv' THEN '1900-01-00'
            when source_name = 'napo_pet_claim_bdx_2022-08-12.csv'
            then '2022-08-01'
            when source_name = 'napo_pet_claim_bdx_2022-08-31.csv'
            then '2022-09-01'
            when source_name = 'napo_pet_claim_bdx_2022-09-30.csv'
            then '2022-10-01'
            when source_name = ' Oct 22'
            then '2022-11-01'
            when source_name = ' Nov 22'
            then '2022-12-01'
            when source_name = 'Dec-22'
            then '2023-01-01'
            when source_name = 'Jan-23'
            then '2023-02-01'
            when source_name = 'Feb-23'
            then '2023-03-01'
            when source_name = 'Mar-23'
            then '2023-04-01'
            -- WHEN Source_Name = 'Apr 23 9 May' THEN '1900-01-00'
            -- WHEN Source_Name = 'Apr 23 9 May' THEN '1900-01-00'
            when source_name = 'Apr 23 Final'
            then '2023-05-01'
            when source_name = 'May 2023 imported by Denys'
            then '2023-06-01'
            when
                source_name
                = 'Napo_Pet_Claim_Bdx_2023 Jun w fix sent - v2 Denys conform columns.csv'
            then '2023-07-01'
            when
                source_name
                = 'Napo_Pet_Claim_Bdx_2023 Jul validation_1 Aug sent - v2 Denys conform columns.csv'
            then '2023-08-01'
            when
                source_name
                = 'Napo_Pet_Claim_Bdx_2023 Aug 1 Sept sent - v2 Denys conform columns.csv'
            then '2023-09-01'
            when
                source_name
                = 'Claims September 2023 to send - v3 - v2 Denys conform columns.csv'
            then '2023-10-01'
            when source_name = 'October 2023 Claims to send v3 Denys fixed dates.csv'
            then '2023-11-01'
            when
                source_name = 'November 2023 Claims Bdx - to send v3 - imported by Ryan'
            then '2023-12-01'
            when source_name = 'December 2023 Claims Bdx - to send - imported by Ewan'
            then '2024-01-01'
            when source_name = 'Claims Bdx Jan 2024 - Internal Upload'
            then '2024-02-01'
        -- add rows here when importing new BDXs --
        end as date
    ) as bdx_nominal_date,
    *
from `raw.claim_bdx_monthly`
where
    source_name
    not in ('napo_pet_claim_bdx_2022-07-01.csv', 'Apr 23 9 May', 'Apr 23 10 May')
