# nwm_retro_indices_transform
This repository contains code for the Google Cloud-based transformation of National Water Model retrospective 3.0 data from AWS S3 Zarr to a BigQuery dataset, and for the creation of a streamflow indices BQ dataset for the 2.7 million NWM reaches computed along the way.


### Set up the resources and infrastructure on Google Cloud
1. Open the Cloud Shell Editor and upload the "cloudinfra_iampolicies_terraform" folder by dragging the folder and dropping it on the File Explorer pane.
2. Replace the placeholder values (inside <>) with actual values of project id and number, developer email, etc. in 'container/cloudbuild.yaml' and 'terraform.tfvars'.
2. Open the Cloud Shell terminal and change the directory to the "cloud_infra_terraform" folder.
    ```
    cd cloudinfra_iampolicies_terraform
    ```
3. Intialize the terraform infrastructure and create an artifact registry repository resource by running the following code in the terminal. [Type 'yes' to allow progressions when asked for.] [If any error appears, retry again for a success]
    ```
    terraform init
    terraform apply -target=google_artifact_registry_repository.repo
    ```
4. Check and deploy the comprehensive terraform infrastructure by running following commands. [Type 'yes' to allow progressions when asked for.] [If any error appears, retry again for a success]
    ```
    terraform plan
    terraform apply
    ```

#### Resources Created
1. Artifact Registry Repository
2. BigQuery Dataset
3. Google Cloud Storage Buckets
4. Vertex AI Notebook Instance
5. IAM Service Accounts

### Run the transform job
