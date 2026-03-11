# PCI DLP API Demo with Google Cloud Dataflow and Cloud KMS

This project demonstrates how to build and run an Apache Beam pipeline on Google Cloud Dataflow that uses the Google Cloud Data Loss Prevention (DLP) API to de-identify sensitive data (like emails, phone numbers, addresses, and names).

Crucially, it uses **Format-Preserving Encryption (FPE)** backed by a **Cloud KMS-wrapped key** for secure, production-ready tokenization.

## Prerequisites

- A Google Cloud Project.
- The Google Cloud SDK (`gcloud` CLI) installed and authenticated.
- Python 3.7+ installed.

## Step 1: Configure GCP Environment and Resources

Set up your active project, enable the required APIs, and create the Cloud Storage buckets for input, output, and Dataflow staging.

```bash
# Set your active project and choose a bucket name
export PROJECT_ID="your-actual-gcp-project-id"
export BUCKET_NAME="your-demo-bucket-name"
gcloud config set project $PROJECT_ID

# Enable the required APIs
gcloud services enable dataflow.googleapis.com \
    dlp.googleapis.com \
    storage.googleapis.com \
    cloudkms.googleapis.com

# Create a Cloud Storage bucket
gcloud storage buckets create gs://$BUCKET_NAME --location=us-central1
gcloud storage buckets create gs://$SECURE_DLQ_BUCKET \
    --project=$PROJECT_ID \
    --location=$REGION \
    --uniform-bucket-level-access \
    --public-access-prevention

# Create some dummy input data and upload it
echo "My name is John Doe and my email is john.doe@example.com." > sample.txt
echo "Contact me at 555-012-3456 or visit 123 Main St." >> sample.txt
gcloud storage cp sample.txt gs://$BUCKET_NAME/input/sample.txt
```

## Step 2: Configure Cloud KMS & Secure the Key

For production-grade security, we use Cloud KMS to wrap the Data Encryption Key (DEK) used by the DLP API.

```bash
# 1. Create a KeyRing and a CryptoKey
gcloud kms keyrings create dlp-demo-keyring --location global
gcloud kms keys create dlp-fpe-key --location global \
    --keyring dlp-demo-keyring --purpose encryption

# 2. Generate a 32-byte random DEK and save it to a local file
openssl rand 32 > dek.bin

# 3. Wrap the DEK using the KMS key
gcloud kms encrypt \
    --location global \
    --keyring dlp-demo-keyring \
    --key dlp-fpe-key \
    --plaintext-file dek.bin \
    --ciphertext-file dek.bin.encrypted

# 4. Encode the encrypted DEK in base64 to pass it to the script
export WRAPPED_KEY=$(base64 -w 0 < dek.bin.encrypted)
export KMS_KEY_NAME="projects/$PROJECT_ID/locations/global/keyRings/dlp-demo-keyring/cryptoKeys/dlp-fpe-key"

# 5. Grant the DLP Service Agent permission to decrypt using the KMS key
# Note: DLP uses a service agent to perform de-identification.
export PROJECT_NUM=$(gcloud projects list --filter="projectId:$PROJECT_ID" --format="value(projectNumber)")
export DLP_SERVICE_AGENT="service-${PROJECT_NUM}@dlp-api.iam.gserviceaccount.com"

gcloud kms keys add-iam-policy-binding dlp-fpe-key \
    --location global \
    --keyring dlp-demo-keyring \
    --member "serviceAccount:$DLP_SERVICE_AGENT" \
    --role "roles/cloudkms.cryptoKeyEncrypterDecrypter"

# Roles needed for SA
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$DLP_SERVICE_AGENT" \
    --role="roles/dataflow.worker"
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$DLP_SERVICE_AGENT" \
    --role="roles/storage.objectCreator" \
    --condition=None
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:225310704867-compute@developer.gserviceaccount.com" \
    --role="roles/serviceusage.serviceUsageConsumer"
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:225310704867-compute@developer.gserviceaccount.com" \
    --role="roles/dlp.user"
```

## Step 3: Local Environment Setup

Set up a Python virtual environment and install the required packages using the `requirements.txt` file.

```bash
# Create a virtual environment and activate it
python3 -m venv venv
source venv/bin/activate

# Install the dependencies
pip install -r requirements.txt
```

## Step 4: Deploy Pipeline to Dataflow

Deploy the Apache Beam pipeline to Dataflow. We pass the `requirements.txt` file so Dataflow workers know to install the required `google-cloud-dlp` library.

```bash
python deidentify.py \
  --input_bucket gs://$BUCKET_NAME/input/*.csv \
  --output_bucket gs://$BUCKET_NAME/output/results \
  --dlq_bucket gs://$SECURE_DLQ_BUCKET/dlq_results \
  --project $PROJECT_ID \
  --region $REGION \
  --runner DataflowRunner \
  --temp_location gs://$BUCKET_NAME/temp \
  --staging_location gs://$BUCKET_NAME/staging \
  --kms_key_name $KMS_KEY_NAME \
  --wrapped_key $WRAPPED_KEY \
  --requirements_file ./requirements.txt
```

## Step 5: Monitor Execution

1. **Google Cloud Console:** Navigate to **Dataflow -> Jobs** in the GCP Console. Click on the active job to view the visual execution graph.
2. **Logs:** Use the bottom panel in the Dataflow job page to inspect Worker Logs and Job Logs for errors or info-level logging output.
3. **Verify Output:** Once the job finishes successfully, verify the redacted output files:
   ```bash
   gcloud storage cat gs://$BUCKET_NAME/output/results-*.txt
   gcloud storage cp gs://$BUCKET_NAME/output/results-*.csv .
   column -s, -t < results-00000-of-00001.csv | head -n 5
   ```
   
