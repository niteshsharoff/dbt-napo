with
    renewals as (
        select
            policy_uuid,
            json_extract_scalar(
                policy, '$.pet.age_in_months_at_start_date'
            ) as pet_age_in_months_at_start_date,
            json_extract_scalar(policy, '$.pet.breed.category') as breed_category,
            json_extract_scalar(policy, '$.pet.breed.name.name') as breed_name,
            json_extract_scalar(policy, '$.pet.breed.name.type') as breed_name_type,
            json_extract_scalar(policy, '$.pet.sex') as pet_sex,
            json_extract_scalar(policy, '$.pet.species') as pet_species,
            json_extract_scalar(policy, '$.pet.cost') as pet_cost,
            json_extract_scalar(policy, '$.pet.is_microchipped') as pet_is_microchipped,
            json_extract_scalar(policy, '$.pet.is_neutered') as pet_is_neutered,
            json_extract_scalar(policy, '$.pet.postal_code') as postal_code,
            json_extract_scalar(policy, '$.pet.quote_source') as quote_source,
            json_extract_scalar(price, '$.price') as retail_price,
            json_extract(price, '$.retail_discount') as retail_discount,
            json_extract_scalar(price, '$.add_ipt.post_price') as gwp_and_ipt,
            json_extract_scalar(
                price, '$.gross_written_premium.premium'
            ) as gross_written_premium,
            json_extract(price, '$.gross_written_premium.cap_and_collar') as cap_collar,
            json_extract_scalar(
                price, '$.gross_written_premium.cap_and_collar.has_claims_cap'
            ) as cap_collar_has_claims_cap,
            json_extract_scalar(
                price, '$.gross_written_premium.cap_and_collar.has_collar'
            ) as cap_collar_has_collar,
            json_extract_scalar(
                price, '$.gross_written_premium.cap_and_collar.has_no_claims_cap'
            ) as cap_collar_has_no_claims_cap,
            json_extract_scalar(
                price,
                '$.gross_written_premium.cap_and_collar.pre_cap_and_collar_premium'
            ) as cap_collar_pre,
            json_extract_scalar(
                price,
                '$.gross_written_premium.cap_and_collar.post_cap_and_collar_premium'
            ) as cap_collar_post,
            json_extract(
                price, '$.gross_written_premium.interproduct_rules'
            ) as interproduct_rules,
            json_extract(
                price, '$.gross_written_premium.interproduct_rules.pre_premium'
            ) as interproduct_rules_pre,
            json_extract(
                price, '$.gross_written_premium.interproduct_rules.post_premium'
            ) as interproduct_rules_post,
            json_extract(price, '$.gross_written_premium.min_premium') as min_premium,
            json_extract(
                price, '$.gross_written_premium.min_premium.pre_premium'
            ) as min_premium_pre,
            json_extract(
                price, '$.gross_written_premium.min_premium.post_premium'
            ) as min_premium_post,
            json_extract(
                price, '$.gross_written_premium.multipet_discount.discounted_percent'
            ) as multipet_discount,
            json_extract(
                price, '$.gross_written_premium.technical_premium.premium'
            ) as technical_premium,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.premium'
            ) as market_technical_premium,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.claims_loading'
            ) as market_technical_premium_claims_loading,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.price'
            ) as market_best_estimate,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_Adj'
            ) as market_raw_adjusted_price,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_AgeSlantDelta'
            ) as market_raw_age_slant_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_BrachyDelta'
            ) as market_raw_brachy_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_SpeciesBreedDelta'
            ) as market_raw_breed_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_SpeciesBreedAgeDelta'
            ) as market_raw_breed_age_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_ConfusedMBAndCB'
            ) as market_raw_confused_breed_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_MBSize'
            ) as market_raw_mixed_size_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_BR'
            ) as market_raw_base_rate,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.Price_ModelOutput'
            ) as market_raw_model_price,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw'
            ) as market_raw_output,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.premium'
            ) as claim_technical_premium,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.target_loss_ratio'
            ) as claim_technical_premium_target_loss_ratio,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.cost'
            ) as claim_best_estimate,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.adjust_cost.multiplier'
            ) as claim_best_estimate_adjust_cost_multiplier,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.frequency_best_estimate.frequency'
            ) as claim_vetbills_frequency_raw,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.frequency_best_estimate.raw'
            ) as claim_vetbills_frequency_factors,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.severity_best_estimate.cost'
            ) as claim_vetbills_severity_raw,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.severity_best_estimate.raw'
            ) as claim_vetbills_severity_factors,
            product_reference,
            version
        from
            `ae32-vpcservice-datawarehouse.raw.pricing_algorithm_book_analysis_renewals`
    ),
    new_business as (
        select
            policy_uuid,
            json_extract_scalar(
                pet, '$.age_in_months_at_start_date'
            ) as pet_age_in_months_at_start_date,
            json_extract_scalar(pet, '$.breed.category') as breed_category,
            json_extract_scalar(pet, '$.breed.name.name') as breed_name,
            json_extract_scalar(pet, '$.breed.name.type') as breed_name_type,
            json_extract_scalar(pet, '$.sex') as pet_sex,
            json_extract_scalar(pet, '$.species') as pet_species,
            json_extract_scalar(pet, '$.cost') as pet_cost,
            json_extract_scalar(pet, '$.is_microchipped') as pet_is_microchipped,
            json_extract_scalar(pet, '$.is_neutered') as pet_is_neutered,
            json_extract_scalar(pet, '$.postal_code') as postal_code,
            json_extract_scalar(pet, '$.quote_source') as quote_source,
            json_extract_scalar(price, '$.price') as retail_price,
            json_extract(price, '$.retail_discount') as retail_discount,
            json_extract_scalar(price, '$.add_ipt.post_price') as gwp_and_ipt,
            json_extract_scalar(
                price, '$.gross_written_premium.premium'
            ) as gross_written_premium,
            json_extract(price, '$.gross_written_premium.cap_and_collar') as cap_collar,
            json_extract_scalar(
                price, '$.gross_written_premium.cap_and_collar.has_claims_cap'
            ) as cap_collar_has_claims_cap,
            json_extract_scalar(
                price, '$.gross_written_premium.cap_and_collar.has_collar'
            ) as cap_collar_has_collar,
            json_extract_scalar(
                price, '$.gross_written_premium.cap_and_collar.has_no_claims_cap'
            ) as cap_collar_has_no_claims_cap,
            json_extract_scalar(
                price,
                '$.gross_written_premium.cap_and_collar.pre_cap_and_collar_premium'
            ) as cap_collar_pre,
            json_extract_scalar(
                price,
                '$.gross_written_premium.cap_and_collar.post_cap_and_collar_premium'
            ) as cap_collar_post,
            json_extract(
                price, '$.gross_written_premium.interproduct_rules'
            ) as interproduct_rules,
            json_extract(
                price, '$.gross_written_premium.interproduct_rules.pre_premium'
            ) as interproduct_rules_pre,
            json_extract(
                price, '$.gross_written_premium.interproduct_rules.post_premium'
            ) as interproduct_rules_post,
            json_extract(price, '$.gross_written_premium.min_premium') as min_premium,
            json_extract(
                price, '$.gross_written_premium.min_premium.pre_premium'
            ) as min_premium_pre,
            json_extract(
                price, '$.gross_written_premium.min_premium.post_premium'
            ) as min_premium_post,
            json_extract(
                price, '$.gross_written_premium.multipet_discount.discounted_percent'
            ) as multipet_discount,
            json_extract(
                price, '$.gross_written_premium.technical_premium.premium'
            ) as technical_premium,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.premium'
            ) as market_technical_premium,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.claims_loading'
            ) as market_technical_premium_claims_loading,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.price'
            ) as market_best_estimate,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_Adj'
            ) as market_raw_adjusted_price,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_AgeSlantDelta'
            ) as market_raw_age_slant_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_BrachyDelta'
            ) as market_raw_brachy_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_SpeciesBreedDelta'
            ) as market_raw_breed_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_SpeciesBreedAgeDelta'
            ) as market_raw_breed_age_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_ConfusedMBAndCB'
            ) as market_raw_confused_breed_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_MBSize'
            ) as market_raw_mixed_size_adj,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.PriceAfter_BR'
            ) as market_raw_base_rate,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw.output.Price_ModelOutput'
            ) as market_raw_model_price,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.market_technical_premium.adjusted_market_best_estimate.raw'
            ) as market_raw_output,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.premium'
            ) as claim_technical_premium,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.target_loss_ratio'
            ) as claim_technical_premium_target_loss_ratio,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.cost'
            ) as claim_best_estimate,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.adjust_cost.multiplier'
            ) as claim_best_estimate_adjust_cost_multiplier,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.frequency_best_estimate.frequency'
            ) as claim_vetbills_frequency_raw,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.frequency_best_estimate.raw'
            ) as claim_vetbills_frequency_factors,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.severity_best_estimate.cost'
            ) as claim_vetbills_severity_raw,
            json_extract(
                price,
                '$.gross_written_premium.technical_premium.claim_technical_premium.cost_best_estimate.vet_cost_best_estimate.severity_best_estimate.raw'
            ) as claim_vetbills_severity_factors,
            product_reference,
            version
        from
            `ae32-vpcservice-datawarehouse.raw.pricing_algorithm_book_analysis_new_business`
    ),
    all_business as (
        select renewals.*, 'renewals' as dump_type
        from renewals
        union all
        select new_business.*, 'new_business' as dump_type
        from new_business
    )
select p.policy.policy_id, all_business.*
from all_business
inner join
    (
        select *
        from `ae32-vpcservice-datawarehouse.dbt.int_policy_history`
        where row_effective_to > current_timestamp()
    ) p
    on p.policy.uuid = all_business.policy_uuid
