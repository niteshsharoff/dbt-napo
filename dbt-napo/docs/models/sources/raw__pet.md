{% docs raw__pet %}

This should eventually be consistent with the policy_pet table in Policy Service's Postgres DB.

The raw data is incrementally loaded into GCS daily based on the `change_at` column and partitioned by day.

{% enddocs %}