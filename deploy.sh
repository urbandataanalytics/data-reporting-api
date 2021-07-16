#!/bin/bash

# -----------------------------------------------
# Building cloud function service

PROJECT_ID=reporting-320010 
REGION=europe-west1
BUCKET_NAME=data-etl

#gsutil mb gs://${BUCKET_NAME}/public/

gcloud functions deploy report_execution \
  --source functions/report_execution/ \
  --region $REGION \
  --entry-point 'main' \
  --memory 512 \
  --timeout '30s' \
  --max-instances 2 \
  --runtime python37 \
  --trigger-http
