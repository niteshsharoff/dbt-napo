{% docs stg_raw__policy_ledger %}

Staging table based on the policy ledger implemented in Django. Additional timestamp columns are added to help identify the transaction time of cancellations and reinstatements.

An example on how the extended ledger timestamps are computed

|  reference_number |  annual_payment_id |  subscription_id |  cancel_date |  _sold_grp |  _cancel_grp |  _reinstate_grp |  sold_at                       |  cancelled_at                  |  reinstated_at                 |  effective_from                |   |
|-------------------|--------------------|------------------|--------------|------------|--------------|-----------------|--------------------------------|--------------------------------|--------------------------------|--------------------------------|---|
| HAR-BET-0020      | null               | SB000T9D2SV182   | null         |          1 |            1 |               1 | 2022-11-21 21:45:55.013000 UTC | 2023-02-24 11:02:48.973000 UTC | 2023-03-09 12:26:22.446000 UTC | 2023-03-09 12:30:37.038000 UTC |   |
| HAR-BET-0020      | null               | SB000QQT0K9A3Z   | null         |          1 |            1 |               1 | 2022-11-21 21:45:55.013000 UTC | 2023-02-24 11:02:48.973000 UTC | 2023-03-09 12:26:22.446000 UTC | 2023-03-09 12:26:22.446000 UTC |   |
| HAR-BET-0020      | null               | SB000QQT0K9A3Z   | 2023-01-22   |          1 |            1 |               0 | 2022-11-21 21:45:55.013000 UTC | 2023-02-24 11:02:48.973000 UTC | null                           | 2023-02-24 11:02:48.973000 UTC |   |
| HAR-BET-0020      | null               | SB000QQT0K9A3Z   | null         |          1 |            0 |               0 | 2022-11-21 21:45:55.013000 UTC | null                           | null                           | 2022-11-21 21:46:05.496000 UTC |   |
| HAR-BET-0020      | null               | SB000QQT0K9A3Z   | null         |          1 |            0 |               0 | 2022-11-21 21:45:55.013000 UTC | null                           | null                           | 2022-11-21 21:45:55.013000 UTC |   |
| HAR-BET-0020      | null               | null             | null         |          0 |            0 |               0 | null                           | null                           | null                           | 2022-11-21 21:45:54.652000 UTC |   |
| HAR-BET-0020      | null               | null             | null         |          0 |            0 |               0 | null                           | null                           | null                           | 2022-11-21 21:43:13.304000 UTC |   |

An example of a policy with multiple cancellations

| Row |  reference_number |  annual_payment_id |  subscription_id |  cancel_date |  _sold_grp |  _cancel_grp |  _reinstate_grp |  sold_at                       |  cancelled_at                  |  reinstated_at                 |  effective_from                |   |
|-----|-------------------|--------------------|------------------|--------------|------------|--------------|-----------------|--------------------------------|--------------------------------|--------------------------------|--------------------------------|---|
|   1 | ADV-KOD-0007      | null               | SB000J76Y0GXBY   | 2022-10-25   |          1 |            2 |               1 | 2022-02-24 00:44:25.548000 UTC | 2023-01-29 10:33:13.112000 UTC | 2023-01-16 15:30:00.404000 UTC | 2023-01-29 10:33:13.112000 UTC |   |
|   2 | ADV-KOD-0007      | null               | SB000J76Y0GXBY   | null         |          1 |            1 |               1 | 2022-02-24 00:44:25.548000 UTC | 2022-12-21 03:01:08.953000 UTC | 2023-01-16 15:30:00.404000 UTC | 2023-01-16 15:30:00.404000 UTC |   |
|   3 | ADV-KOD-0007      | null               | SB000J76Y0GXBY   | 2022-09-25   |          1 |            1 |               0 | 2022-02-24 00:44:25.548000 UTC | 2022-12-21 03:01:08.953000 UTC | null                           | 2022-12-21 03:01:08.953000 UTC |   |
|   4 | ADV-KOD-0007      | null               | SB000J76Y0GXBY   | null         |          1 |            0 |               0 | 2022-02-24 00:44:25.548000 UTC | null                           | null                           | 2022-02-24 00:44:25.548000 UTC |   |

{% enddocs %}