from datetime import datetime
import firebase_admin
from firebase_admin import firestore
from google.cloud import bigquery
import pandas as pd
import functions_framework

@functions_framework.http
def main(request):
    dataset_name='firestore'

    app = firebase_admin.initialize_app()
    db = firestore.client()

    def manipulate(x):
        temp =x.to_dict()
        temp['id']=x.id
        return temp

    mapping = {
        'user':'users',
        'booking':'bookings',
        'emailReminders':'email_reminders',
        'smsReminders':'sms_reminders'
    }

    for key in mapping:
        print(mapping[key])

    for key in mapping:
        temp = list(db.collection('{}'.format(key)).stream())
        temp_dict = list(map(manipulate, temp))
        globals()[f"df_{mapping[key]}"] = pd.DataFrame(temp_dict)
        globals()[f"df_{mapping[key]}"]['_imported_at'] = datetime.now()

    client = bigquery.Client(project="ae32-vpcservice-datawarehouse",)
    job_config2 = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )


    for key in mapping:
        load_job = client.load_table_from_dataframe(
            globals()[f"df_{mapping[key]}"]
            ,"ae32-vpcservice-datawarehouse.{}.{}".format(dataset_name,mapping[key])
            ,job_config=job_config2
            ,location='EU'
        ) 
        print(load_job.result())

    return f'success'

