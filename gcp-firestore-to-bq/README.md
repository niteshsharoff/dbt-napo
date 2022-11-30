# Firestore to BigQuery import - Cloud function

## Reasons for building a function instead of using native functionality
Although Firestore has a native BigQuery export extension, we encountered two restrictions with this:
1) You can only export one collection with the extension
2) The extension doesn't export retrospective records. (Modified and created records are streamed from the point of setting up the stream, historical data isn't available)

To backfill the missing data, the recommendation is to use [these instructions](https://github.com/firebase/extensions/blob/master/firestore-bigquery-export/guides/IMPORT_EXISTING_DOCUMENTS.md), but we couldn't get this to work.


## Pre-requisits for this function
- Ensure the App Engine default service account is added to the `ae32-vpcservice-datawarehouse` project
  - Permissions to add: `BigQuery Data Editor`, `BigQuery Job User`
- Create a Cloud scheduler to trigger the endpoint daily at 5am
- Ensure the `dataset_name` is populated with the dataset you want in BigQuery (currently `firestore`)

