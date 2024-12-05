#!/bin/bash

PROJECT_ID="your-project-id"
REGION="us-central1"
SERVICE_NAME="fable-api"
ARTIFACT_REGISTRY="us-central1-docker.pkg.dev"
REPOSITORY_NAME="fable"

set -e

echo "Starting deployment process..."

echo "Building Docker image..."
docker build -t $SERVICE_NAME .

echo "Configuring Docker for Artifact Registry..."
gcloud auth configure-docker $ARTIFACT_REGISTRY

IMAGE_PATH="$ARTIFACT_REGISTRY/$PROJECT_ID/$REPOSITORY_NAME/$SERVICE_NAME"
echo "Tagging image as: $IMAGE_PATH"
docker tag $SERVICE_NAME $IMAGE_PATH

echo "Pushing image to Artifact Registry..."
docker push $IMAGE_PATH

echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_PATH \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --set-env-vars="ENVIRONMENT=production,GOOGLE_CLOUD_PROJECT=$PROJECT_ID,GOOGLE_CLOUD_REGION=$REGION"

echo "Deployment complete! Service URL:"
gcloud run services describe $SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --format 'value(status.url)'