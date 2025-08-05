# cloudkeeper-gcp-automation
This repository consist of automations to ease google cloud integrations and infra cost optimisations

Setup Instructions:
Install required Python packages:

```bash
pip install google-auth google-api-python-client google-cloud-bigquery requests
```
Download the Script:
Clone the repository:
```bash
git clone https://github.com/CloudKeeper-Inc/cloudkeeper-gcp-automation.git
cd cloudkeeper-gcp-automation
```
Configuration:
Open audit_log_to_bigquery.py and update the following variables:
```bash
PROJECT_ID = "<your-project-id>"
DATASET_ID = "<your-dataset-name>"
ORGANIZATION_ID = "<your-org-id>"
SINK_NAME = "<your-sink-name>"
DATASET_LOCATION = "<your-dataset-location>"  # e.g., "US"
```
Run the Script
```bash
python3 audit_log_to_bigquery.py
```
What It Does:

  Enables audit logs for the Analytics Hub API.
  
  Creates the BigQuery dataset if it doesn’t exist.
  
  Creates or updates the organization-level log sink.
  
  Outputs the sink’s writer identity.

Final Step:
Manually assign roles/bigquery.dataEditor to the sink’s service account:
```bash
service-<org_id>@gcp-sa-logging.iam.gserviceaccount.com
```
This allows the sink to write logs to your BigQuery dataset.

