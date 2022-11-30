import firebase_admin
from firebase_admin import firestore
from google.cloud import bigquery
import pandas as pd
#from firebase_admin import credentials
#from google.oauth2 import service_account
#cred = credentials.Certificate("service-account.json")
#app = firebase_admin.initialize_app(cred)
import functions_framework

@functions_framework.http
def main(request):
    dataset_name='firestore'

    app = firebase_admin.initialize_app()
    db = firestore.client()

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
        temp_dict = list(map(lambda x: x.to_dict(), temp))
        globals()[f"df_{mapping[key]}"] = pd.DataFrame(temp_dict)

    #key_path = r"C:\Users\nites\OneDrive\Documents\napo-nitesh-local-ae32-vpcservice-datawarehouse-879a160a28ee.json"
    #credentials = service_account.Credentials.from_service_account_file(
    #    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    #client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

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

