# ETL run report - `20261029T214644Z-a620b1`

- **Status:** success
- **Files processed:** 16
- **Rows read:** 11,917
- **Rows loaded:** 11,297
- **Rows quarantined:** 420
- **FX source:** fallback

## Data-quality checks

| entity | rule | severity | failed_rows | total_rows | pass_rate |
|---|---|---|---|---|---|
| customer | customer_id_present | error | 2 | 540 | 99.63% |
| customer | customer_id_format | error | 2 | 540 | 99.63% |
| customer | updated_at_valid | error | 0 | 540 | 100.00% |
| customer | email_valid | warn | 14 | 540 | 97.41% |
| customer | dob_valid_adult | warn | 2 | 540 | 99.63% |
| customer | segment_known | warn | 0 | 540 | 100.00% |
| customer | superseded_versions_dropped | info | 30 | 538 |  |
| transaction | exact_duplicates_removed | info | 112 | 11377 |  |
| transaction | txn_id_present | error | 30 | 11265 | 99.73% |
| transaction | customer_exists | error | 79 | 11265 | 99.30% |
| transaction | timestamp_valid | error | 19 | 11265 | 99.83% |
| transaction | timestamp_not_future | error | 29 | 11265 | 99.74% |
| transaction | amount_present | error | 187 | 11265 | 98.34% |
| transaction | amount_positive | error | 56 | 11265 | 99.50% |
| transaction | currency_supported | error | 26 | 11265 | 99.77% |
| transaction | txn_type_valid | error | 0 | 11265 | 100.00% |
| transaction | channel_valid | warn | 0 | 11265 | 100.00% |
| transaction | status_valid | warn | 0 | 11265 | 100.00% |
| transaction | superseded_versions_dropped | info | 58 | 10847 |  |
