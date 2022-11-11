from datetime import timedelta, datetime
from types import MappingProxyType
from typing import Optional, Tuple

import numpy as np
import pandas as pd

IPT_PERCENT = 12
CANCELLATION_MAP = MappingProxyType(
    {
        1: "customer_request",
        2: "fraud",
        3: "cancelled_by_provider",
        4: "Pet passed away",
        5: "No longer has pet",
        6: "Policy bought in error",
        7: "Declined claim",
        8: "Poor experience",
        9: "No inbound phone line",
        10: "Financial reasons",
        11: "Found cheaper alternative",
        12: "Other",
        13: "Inactive mandate",
        14: "Failed payment",
    }
)


def is_sold(annual_payment_id: pd.Series, subscription_id: pd.Series) -> pd.Series:
    return annual_payment_id.notnull() | subscription_id.notnull()


def is_cancelled(
    cancel_date: pd.Series, sold: pd.Series, report_window: Tuple[datetime, datetime]
) -> pd.Series:
    t0 = report_window[0].timestamp() * 1000
    t1 = report_window[1].timestamp() * 1000
    return cancel_date.notna() & (cancel_date >= t0) & (cancel_date <= t1) & sold


def is_renewal(policy_id: pd.Series, renewal_policy_id: pd.Series) -> pd.Series.bool:
    return policy_id.isin(renewal_policy_id.values)


def is_multipet(multipet_number: pd.Series) -> pd.Series:
    return multipet_number.notnull()


def format_status(renewal: bool, cancelled: bool) -> str:
    return "Cancel" if cancelled else "Renewal" if renewal else "New Policy"


def format_name(first_name: pd.Series, last_name: pd.Series) -> pd.Series:
    return first_name + " " + last_name


def format_dob(date_of_birth: pd.Series) -> pd.Series:
    return pd.to_datetime(date_of_birth, format="%Y-%m-%d")


def format_date(column: pd.Series) -> pd.Series:
    return pd.to_datetime(column, unit="ms").dt.strftime("%Y-%m-%d")


def format_breed(breed_name: str, breed_category: str) -> str:
    if breed_category == "mixed":
        return "mixed"

    if breed_category == "pedigree":
        # leverage the condition where na == na always return False
        if breed_name == breed_name:
            return breed_name
        else:
            return "unknown"

    if breed_category == "cross":
        if breed_name == breed_name:
            return "{breed_name} + cross".format(breed_name=breed_name)
        else:
            return "cross"


def format_gender(pet_gender: int) -> str:
    return "male" if pet_gender == 1 else "female"


def format_multipet(multipet: bool) -> str:
    return "Yes" if multipet else "No"


def format_value_by_cancellation(cancelled: bool, value: float) -> str:
    return "-{:.2f}".format(value) if cancelled else "{:.2f}".format(value)


def format_current_source(renewal: bool, quote_source: str) -> str:
    return "renewal" if renewal else quote_source


# TODO: Optimize
def get_original_source(
    renewal: bool,
    quote_source: str,
    policy_id: int,
    renewals: pd.DataFrame,
    quote_sources: pd.DataFrame,
) -> str:
    if renewal:
        original_policy_id = get_original_policy_id(policy_id, renewals)
        return quote_sources[
            quote_sources.policy_id == original_policy_id
        ].quote_source.item()
    else:
        return quote_source


def get_original_policy_id(policy_id: int, renewals: pd.DataFrame) -> int:
    if policy_id in renewals.new_policy_id.values:
        df = renewals[renewals.new_policy_id == policy_id]
        old_policy_id = df.old_policy_id.iloc[0]
        return get_original_policy_id(old_policy_id, renewals)

    return policy_id


def get_transaction_date(cancelled: bool, created_date: int, cancel_date: int) -> str:
    if cancelled:
        return pd.to_datetime(cancel_date, unit="ms").strftime("%Y-%m-%d")

    return pd.to_datetime(created_date, unit="ms").strftime("%Y-%m-%d")


def get_effective_date(cancelled: bool, start_date: int, cancel_date: int) -> str:
    # TODO: Used for new report
    # if cancelled:
    #     return pd.to_datetime(cancel_date, unit="ms").strftime("%Y-%m-%d")

    return pd.to_datetime(start_date, unit="ms").strftime("%Y-%m-%d")


def get_cancellation_date(cancelled: bool, cancel_date: int) -> Optional[str]:
    if cancelled:
        return pd.to_datetime(cancel_date, unit="ms").strftime("%Y-%m-%d")

    return None


def get_cancellation_reason(cancel_reason: int) -> Optional[str]:
    return CANCELLATION_MAP.get(cancel_reason, "")


def calculate_annual_price(
    cancel_date: Optional[int],
    start_date: int,
    annual_price: float,
) -> float:
    cancel_date = pd.to_datetime(cancel_date, unit="ms", utc=True)
    start_date = pd.to_datetime(start_date, unit="ms", utc=True)
    if cancel_date and cancel_date >= start_date + timedelta(days=14):
        policy_cost = (cancel_date - start_date).days / 365 * annual_price
        policy_cost = float("{:.2f}".format(policy_cost))
        return annual_price - policy_cost

    return annual_price


def calculate_price_exc_ipt(price: pd.Series) -> pd.Series:
    return price / (1 + IPT_PERCENT / 100)


def transform_policy_records(
    df: pd.DataFrame,
    renewals: pd.DataFrame,
    quote_sources: pd.DataFrame,
) -> pd.DataFrame:
    df["status"] = np.vectorize(format_status)(df.is_renewal, df.is_cancelled)
    df["current_source"] = np.vectorize(format_current_source)(
        df.is_renewal,
        df.quote_source,
    )
    df["original_source"] = df.apply(
        lambda policy: get_original_source(
            policy.is_renewal,
            policy.quote_source,
            policy.policy_id,
            renewals,
            quote_sources,
        ),
        axis=1,
    )
    df["discount_campaign"] = None  # TBD
    df["annual_price"] = np.vectorize(calculate_annual_price)(
        df.cancel_date,
        df.start_date,
        df.annual_price,
    )
    df["gross_premium_ipt_exc"] = calculate_price_exc_ipt(df.annual_price)
    df["gross_premium_ipt_inc"] = df.annual_price
    df["ipt_percent"] = IPT_PERCENT
    df["ipt"] = df.annual_price - df.gross_premium_ipt_exc
    df["comission_type"] = "new annual"
    df["comission_rate"] = "35%"
    df["napo_annual_commision"] = 0.35 * df.gross_premium_ipt_exc
    df["underwriter_premium_ipt_exc"] = 0.65 * df.gross_premium_ipt_exc
    df["underwriter_premium_ipt_inc"] = 0.65 * df.gross_premium_ipt_exc + df.ipt
    df["issued_date"] = format_date(df.created_date)
    df["effective_date"] = np.vectorize(get_effective_date)(
        df.is_cancelled,
        df.start_date,
        df.cancel_date,
    )
    df["transaction_date"] = np.vectorize(get_transaction_date)(
        df.is_cancelled,
        df.created_date,
        df.cancel_date,
    )
    df["start_date"] = format_date(df.start_date)
    df["end_date"] = format_date(df.end_date)
    df["cancellation_date"] = np.vectorize(get_cancellation_date)(
        df.is_cancelled,
        df.cancel_date,
    )
    df["cancellation_reason"] = np.vectorize(get_cancellation_reason)(df.cancel_reason)
    df["policy_year"] = 1  # TODO: Factor in renewals
    df["payment_method"] = "Direct Debit"

    df.gross_premium_ipt_exc = np.vectorize(format_value_by_cancellation)(
        df.is_cancelled,
        df.gross_premium_ipt_exc,
    )
    df.gross_premium_ipt_inc = np.vectorize(format_value_by_cancellation)(
        df.is_cancelled,
        df.gross_premium_ipt_inc,
    )
    df.ipt = np.vectorize(format_value_by_cancellation)(
        df.is_cancelled,
        df.ipt,
    )
    df.napo_annual_commision = np.vectorize(format_value_by_cancellation)(
        df.is_cancelled,
        df.napo_annual_commision,
    )
    df.underwriter_premium_ipt_exc = np.vectorize(format_value_by_cancellation)(
        df.is_cancelled,
        df.underwriter_premium_ipt_exc,
    )
    df.underwriter_premium_ipt_inc = np.vectorize(format_value_by_cancellation)(
        df.is_cancelled,
        df.underwriter_premium_ipt_inc,
    )
    return df


def format_policy_records(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["Policy Number"] = df.reference_number
    out["Quote Reference"] = df.quote_id
    out["Status"] = df.status
    out["Original Source"] = df.original_source
    out["Current Source"] = df.current_source
    out["Discount Campaign"] = df.discount_campaign  # JP - friends or family disount?
    out["Insured Name"] = df.insured_name
    out["Insured DOB"] = df.insured_dob
    out["Postcode"] = df.postal_code
    out["Policy Type"] = df.product_reference
    out["Unique Pet ID"] = df.pet_id
    out["Pet Type"] = df.pet_type
    out["Pet Name"] = df.pet_name
    out["Pet Breed"] = df.pet_breed
    out["Pet Age (months)"] = df.age_months
    out["Pet Gender"] = df.pet_gender
    out["Pet Cost"] = df.pet_cost
    out["Pet Chipped"] = df.is_microchipped
    out["Pet Neutered"] = df.is_neutered
    out["Multipet"] = df.multipet
    out["Transaction Date"] = df.transaction_date
    out["Effective Date"] = df.effective_date
    out["Original Issue Date"] = df.issued_date
    out["Start Date"] = df.start_date
    out["End Date"] = df.end_date
    out["Policy Year"] = 1  # confirm that this is 1/2/3/4/ etc. not date year
    out["Cancellation Date"] = df.cancellation_date
    out["Cancellation Reason"] = df.cancellation_reason
    out["Payment Method"] = df.payment_method
    out["Payment Period"] = df.payment_plan_type
    out["Gross Premium IPT exc"] = df.gross_premium_ipt_exc
    out["Gross Premium IPT inc"] = df.gross_premium_ipt_inc
    out["IPT %"] = df.ipt_percent
    out["IPT"] = df.ipt
    out["Commision Type"] = df.comission_type
    out["Commision Rate"] = df.comission_rate
    out["Napo Annual Commision (35%)"] = df.napo_annual_commision
    out["Net Rated Premium Due To Underwriter IPT exc"] = df.underwriter_premium_ipt_exc
    out["Total Due To Underwriter IPT inc"] = df.underwriter_premium_ipt_inc
    out["Primary Customer Id"] = df.customer_id
    out["Policy Id"] = df.policy_id
    return out
