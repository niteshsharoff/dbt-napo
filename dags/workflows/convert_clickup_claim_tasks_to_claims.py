import pandas as pd
from pydantic import BaseModel
from typing import Any, Optional, List, Dict
import json


class TaskDropDownConfigOption(BaseModel):
    id: str
    name: str


class TaskDropDownConfig(BaseModel):
    options: List[TaskDropDownConfigOption]


class TaskCustomField(BaseModel):
    type_config: Any = None
    value: Any = None
    field_type: str
    name: str

    class Config:
        fields = {"field_type": "type"}


class TaskStatus(BaseModel):
    status: str


def extract_custom_field_value(custom_field: Optional[TaskCustomField]) -> Any:
    if custom_field is None or custom_field.value is None:
        return None
    if custom_field.field_type == "drop_down" and custom_field.value is not None:
        type_config = TaskDropDownConfig.parse_obj(custom_field.type_config)
        return type_config.options[custom_field.value].name
    return custom_field.value


def construct_custom_field_lookup(
    custom_field_dicts: List[Any],
) -> Dict[str, TaskCustomField]:
    task_custom_fields_lookup: Dict[str, TaskCustomField] = {}
    for custom_field_dict in custom_field_dicts:
        custom_field = TaskCustomField.parse_obj(custom_field_dict)
        task_custom_fields_lookup[custom_field.name] = custom_field
    return task_custom_fields_lookup


def convert_clickup_claim_tasks_to_claims(
    customer_tasks: pd.DataFrame, vet_tasks: pd.DataFrame
) -> pd.DataFrame:
    tasks_df = pd.concat(
        [customer_tasks.assign(source="customer"), vet_tasks.assign(source="vet")]
    )
    claims = []
    for _, task in tasks_df.iterrows():
        task_status = TaskStatus.parse_obj(json.loads(task.status))
        task_custom_fields_lookup = construct_custom_field_lookup(
            json.loads(task.custom_fields)
        )
        # Restrict the columns we export to limit sensitive data leaking into data warehouse
        claim = {
            "id": task.custom_id,
            "status": task_status.status,
            "policy_id": extract_custom_field_value(
                task_custom_fields_lookup.get("policy_id")
            ),
            "master_claim_id": extract_custom_field_value(
                task_custom_fields_lookup.get("Master Claim ID")
            ),
            "date_received": extract_custom_field_value(
                task_custom_fields_lookup.get("Date Received")
            ),
            "onset_date": extract_custom_field_value(
                task_custom_fields_lookup.get("Onset Date")
            ),
            "type": extract_custom_field_value(
                task_custom_fields_lookup.get("cover_type")
            ),
            "sub_type": extract_custom_field_value(
                task_custom_fields_lookup.get("Claim Sub type")
            ),
            "paid_amount": extract_custom_field_value(
                task_custom_fields_lookup.get("paid_amount")
            ),
            "first_invoice_date": extract_custom_field_value(
                task_custom_fields_lookup.get("First invoice date")
            ),
            "decline_reason": extract_custom_field_value(
                task_custom_fields_lookup.get("Decline Reason")
            ),
            "invoice_amount": extract_custom_field_value(
                task_custom_fields_lookup.get("invoice_amount")
            ),
            "is_continuation": extract_custom_field_value(
                task_custom_fields_lookup.get("Continuation")
            ),
            "source": task.source
        }
        claims += [claim]

    return pd.DataFrame(claims)
