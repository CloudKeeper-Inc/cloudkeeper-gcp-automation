import json
import google.auth
from google.cloud import bigquery
from google.auth.transport.requests import Request
from googleapiclient import discovery
import requests


# Constants
PROJECT_ID = "<your-project-id>"     # project that you have created for us for billing export
DATASET_ID = "audit_logs_dataset"       # Name of dataset to create
ORGANIZATION_ID = "<your-org-id>"       # Your GCP Org ID
SINK_NAME = "audit_sink"                # Name of the log sink
DATASET_LOCATION = "US"                 # Dataset region
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def get_credentials():
   creds, _ = google.auth.default(scopes=SCOPES)
   return creds


def enable_org_audit_logs(credentials, organization_id, service_name):
   crm_service = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)
   org_resource = f'organizations/{organization_id}'
   policy = crm_service.organizations().getIamPolicy(resource=org_resource, body={}).execute()
   audit_configs = policy.get('auditConfigs', [])


   service_audit_config = next((ac for ac in audit_configs if ac.get('service') == service_name), None)


   if not service_audit_config:
       service_audit_config = {'service': service_name, 'auditLogConfigs': []}
       audit_configs.append(service_audit_config)


   log_types = ['ADMIN_READ', 'DATA_WRITE', 'DATA_READ']
   existing_log_types = {alc['logType'] for alc in service_audit_config.get('auditLogConfigs', [])}


   for log_type in log_types:
       if log_type not in existing_log_types:
           service_audit_config['auditLogConfigs'].append({'logType': log_type})


   policy['auditConfigs'] = audit_configs
   set_policy_request = {'policy': policy}
   crm_service.organizations().setIamPolicy(resource=org_resource, body=set_policy_request).execute()


   print(f" Enabled audit logs for service '{service_name}' in organization {organization_id}")


def create_bigquery_dataset(credentials):
   client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
   dataset_ref = client.dataset(DATASET_ID)
   try:
       client.get_dataset(dataset_ref)
       print(f" Dataset '{DATASET_ID}' already exists.")
   except Exception:
       dataset = bigquery.Dataset(dataset_ref)
       dataset.location = DATASET_LOCATION
       try:
           client.create_dataset(dataset)
           print(f" Created dataset '{DATASET_ID}'.")
       except Exception as e:
           print(f" Error creating dataset: {e}")
           raise


def get_access_token(credentials):
   credentials.refresh(Request())
   return credentials.token


def create_org_logging_sink(credentials):
   access_token = get_access_token(credentials)
   url = f"https://logging.googleapis.com/v2/organizations/{ORGANIZATION_ID}/sinks?uniqueWriterIdentity=true"


   headers = {
       "Authorization": f"Bearer {access_token}",
       "Content-Type": "application/json"
   }


   destination = f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}"
   filter_expression = (
       'resource.type="bigquery_resource" OR '
       '(resource.type="bigquery_project" AND protoPayload.serviceName="analyticshub.googleapis.com")'
   )


   body = {
       "name": SINK_NAME,
       "destination": destination,
       "filter": filter_expression,
       "includeChildren": True
   }


   response = requests.post(url, headers=headers, data=json.dumps(body))


   if response.status_code == 200:
       sink_info = response.json()
       print(f" Sink '{SINK_NAME}' created successfully!")
       print(f"   Writer identity: {sink_info.get('writerIdentity')}")
   elif response.status_code == 409:
       print(f" Sink '{SINK_NAME}' already exists. Attempting to update the filter...")
       update_org_logging_sink_filter(credentials, destination, filter_expression)
   else:
       print(f" Failed to create sink: {response.status_code}")
       print(response.text)
       response.raise_for_status()


def update_org_logging_sink_filter(credentials, destination, filter_expression):
   access_token = get_access_token(credentials)
   url = f"https://logging.googleapis.com/v2/organizations/{ORGANIZATION_ID}/sinks/{SINK_NAME}"


   headers = {
       "Authorization": f"Bearer {access_token}",
       "Content-Type": "application/json"
   }


   body = {
       "destination": destination,
       "filter": filter_expression,
       "includeChildren": True
   }


   response = requests.patch(url, headers=headers, data=json.dumps(body))


   if response.status_code == 200:
       print(f" Sink '{SINK_NAME}' filter updated successfully.")
   else:
       print(f" Failed to update sink filter: {response.status_code}")
       print(response.text)
       response.raise_for_status()


def get_sink_writer_identity(credentials):
   access_token = get_access_token(credentials)
   url = f"https://logging.googleapis.com/v2/organizations/{ORGANIZATION_ID}/sinks/{SINK_NAME}"


   headers = {
       "Authorization": f"Bearer {access_token}",
       "Content-Type": "application/json"
   }


   response = requests.get(url, headers=headers)


   if response.status_code == 200:
       sink_info = response.json()
       writer_identity = sink_info.get("writerIdentity", "Not Found")
       print(f"\n Sink Writer Identity: {writer_identity}")
       return writer_identity
   else:
       print(f"\n Failed to fetch sink writer identity: {response.status_code}")
       print(response.text)
       return None


def main():
   print(" Starting automation to enable audit logs for BigQuery Analytics Hub...")


   credentials = get_credentials()


   print("\n Step 1: Enabling Audit Logs for Analytics Hub API at the Organization Level...")
   enable_org_audit_logs(credentials, ORGANIZATION_ID, 'analyticshub.googleapis.com')


   print("\n Step 2: Creating BigQuery Dataset (if not exists)...")
   create_bigquery_dataset(credentials)


   print("\n Step 3: Creating or Updating Organization-level Logging Sink...")
   create_org_logging_sink(credentials)


   print("\n Step 4: Fetching Sink Writer Identity...")
   get_sink_writer_identity(credentials)


   print("\n All done!")


if __name__ == "__main__":
   main()
