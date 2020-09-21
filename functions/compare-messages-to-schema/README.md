# Compare messages to schema
Function that compares messages from history buckets against the schema of the topic where the messages come from.

## Setup
1. Make sure a ```config.py``` file exists within the directory with the correct configuration:
    ~~~
    DELEGATED_SA = The GCP Service Account with all necessary rights to check resources
    JIRA_USER = User account for JIRA API
    JIRA_SERVER = URL towards JIRA domain
    JIRA_PROJECT = JIRA project ID for to-be-created issues
    JIRA_BOARD = JIRA board ID for retrieving current sprint for to-be-created issues
    JIRA_EPIC = Epic name for to-be-created issues
    JIRA_SECRET_ID = The ID in the secrets manager which contains the JIRA API key
    ~~~
2. Make sure the following variables are present in the environment:
    ~~~
    SCHEMAS_BUCKET_NAME = The bucket where the schemas can be found that are compared against messages  DATA_CATALOGS_BUCKET_NAME = The bucket where the data catalogs of projects can be found
    PROJECT_ID = The project ID of the project where the JIRA secret key can be found and also the project ID of the project where the delegated service account is from
    ~~~
3. Create a custom Google Cloud Platform role and assign this to the delegated service account (see [Permissions](#permissions));
4. Deploy the function with help of the [cloudbuild.example.yaml](cloudbuild.example.yaml) to the Google Cloud Platform.

## Function
The validate-message function works as follows:
1. The code first lists all data catalogs and checks which data catalog has a topic. It puts the data catalogs with a topic in a list.
2. For every data catalog with a topic, it finds the history bucket belonging to said topic.
3. For every topic, it finds the schema belonging to that topic.
4. For every blob that was put into the history bucket the day before, it unzips the blob and validates every message it finds against the schema of the topic.
5. If a blob has a message that is not conform the schema of its topic, a JIRA ticket is made.


## Permissions
This function depends on a Service Account (hereafter SA) with specific permissions to access project resources. Because the pre-defined roles within the platform do not suit our needs, 
a custom role has to be defined and assigned to the SA. To create a custom role within GCP you can follow [this guide](https://cloud.google.com/iam/docs/creating-custom-roles). 
The custom role must have the following permission on every history bucket:
- `storage.objects.list`: Listing all objects within a bucket

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
