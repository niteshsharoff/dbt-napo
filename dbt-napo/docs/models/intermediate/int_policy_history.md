{% docs int_policy_history %}

Historic normalized full policy data built by joining policy, pet and customer ledger tables together

Given the hypothetical event matrix

| time | policy_tx | pet_tx | customer_tx | tx_valid_from | tx_valid_to |
|------|-----------|--------|-------------|---------------|-------------|
| 0    | 0         |        |             | 0             | 5           |
| 1    |           | 1      |             | 1             | 3           |
| 2    |           |        | 2           | 2             | 4           |
| 3    |           | 3      |             | 3             | ∞           |
| 4    |           |        | 4           | 4             | ∞           |
| 5    | 5         |        |             | 5             | ∞           |

We expect the joint history matrix

| time | policy_tx | pet_tx | customer_tx | valid_from | valid_to |
|------|-----------|--------|-------------|------------|----------|
| 2    | 0         | 1      | 2           | 2          | 3        |
| 3    | 0         | 3      | 2           | 3          | 4        |
| 4    | 0         | 3      | 4           | 4          | 5        |
| 5    | 5         | 3      | 4           | 5          | ∞        |

{% enddocs %}