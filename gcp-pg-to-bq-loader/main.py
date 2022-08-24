import functions_framework

@functions_framework.http
def main(request):
    from datetime import date, time, datetime
    from google.oauth2 import service_account
    from google.cloud import bigquery, storage
    import pandas as pd
    import json

    dev = False
    if (dev):
        key_path = r"C:\Users\nites\OneDrive\Documents\napo-nitesh-local-ae32-vpcservice-datawarehouse-879a160a28ee.json"
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
        storage_client = storage.Client(credentials=credentials, project=credentials.project_id,) 

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    bucket_name='data-warehouse-harbour'
    dataset_name='postgres'
    path_name='policy-service/{}'.format(datetime.now().strftime("%Y-%m-%d"))
    bucket = storage_client.get_bucket(bucket_name)

    table_mapping = {
        "policy.breed.json":"breed",
        "policy.customer.json":"customer",
        "policy.pet.json":"pet",
        "policy.policy.json":"policy",
        "policy.product.json":"product",
        "policy.productline.json":"productline",
        "policy.payment.json":"policy_payment"
    }

    for table in table_mapping:
        uri = "gs://{}/{}/{}".format(bucket_name,path_name,table)
        table_id = "ae32-vpcservice-datawarehouse.{}.{}".format(dataset_name,table_mapping[table])
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            location="EU",  # To match the destination dataset location
            job_config=job_config,
        )
        print(table,load_job.result())

    # Subscription table loading
    blob = bucket.blob("{}/{}".format(path_name,'policy.subscription.json'))
    df = pd.read_json(blob.download_as_string(), lines=True)
    df1 = pd.json_normalize(df['fields'])

    if 'policyid' in df1.columns:
        policy_column = 'policyid' 
    if '_policyid' in df1.columns:
        policy_column = '_policyid'
        df1.rename(columns={'_policyid':'policyid'},inplace=True)

    if (df1.policyid.count()==df.model.count()):
        print('True')
        df_final = pd.merge(df, df1, left_index=True, right_index=True)
        df_final.drop(columns=['fields','failed_payment_events'],inplace=True)

    job_config2 = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_dataframe(
        df_final
        ,"ae32-vpcservice-datawarehouse.{}.subscription".format(dataset_name)
        ,job_config=job_config2
        ,location='EU'
    ) 
    print(load_job.result())

    return f'success'