GCP Cloud Storage -> BigQuery loader
====================================

This loader proceses the GCP Cloud storage bucket raw files and loads them into their respective BigQuery tables.

It's used for Napo Postgres replications.
Given the BQ auto-loader has some issues with types, the pandas is used to manipulate the contents of the subscription table.

Recommended implementation:
- Setup this code as a Cloud function
- Schedule Cloud Scheduler to ping the endpoint daily so the code runs