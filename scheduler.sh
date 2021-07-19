
# -----------------------------------------------
# Setting cloud scheduler to run process

PROJECT_ID=reporting-320010 
REGION=europe-west1
BUCKET_NAME=data-etl

PORT=8080
SERVICE_URL="https://europe-west1-reporting-320010.cloudfunctions.net/report_execution"
SERVICE_NAME=uda-reporting
SERVICE_ACCOUNT="uda-reporting-invoker-sa@uda-yelp.iam.gserviceaccount.com"
METHOD=POST

gcloud iam service-accounts create uda-reporting-invoker-sa \
   --display-name "$PROJECT_ID service account"

gcloud run services add-iam-policy-binding $SERVICE_NAME \
   --member=serviceAccount$SERVICE_ACCOUNT \
   --role=roles/run.invoker

gcloud scheduler jobs create http slack-exec-report-job --schedule "0 * * * *" \
   --http-method=$METHOD \
   --uri=$SERVICE_URL \
   --oidc-service-account-email=$SERVICE_ACCOUNT   \
   --oidc-token-audience=$SERVICE_URL
