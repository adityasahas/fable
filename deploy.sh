#!/bin/bash

PROJECT_ID="fable-adi"
REGION="us-central1"
SERVICE_NAME="fable-api"
ARTIFACT_REGISTRY="us-central1-docker.pkg.dev"
REPOSITORY_NAME="fable"

# Function to handle errors
handle_error() {
    echo "An error occurred at $(date). Press any key to exit..."
    read -n 1 -s
    exit 1
}

# Set error handler
trap 'handle_error' ERR

set -e

echo "Starting deployment process at $(date)..."

echo "Building Docker image (this may take several minutes)..."
docker build \
    --network=host \
    -t $SERVICE_NAME \
    --build-arg GOOGLE_CLOUD_PROJECT=$PROJECT_ID \
    . || handle_error
echo "Docker build completed at $(date)"

echo "Configuring Docker for Artifact Registry..."
gcloud auth configure-docker $ARTIFACT_REGISTRY --quiet || handle_error

IMAGE_PATH="$ARTIFACT_REGISTRY/$PROJECT_ID/$REPOSITORY_NAME/$SERVICE_NAME"
echo "Tagging image as: $IMAGE_PATH"
docker tag $SERVICE_NAME $IMAGE_PATH || handle_error

echo "Pushing image to Artifact Registry (this may take several minutes)..."
docker push $IMAGE_PATH || handle_error

echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_PATH \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --quiet \
  --timeout=15m \
  --set-env-vars="ENVIRONMENT=production,GOOGLE_CLOUD_PROJECT=$PROJECT_ID,GOOGLE_CLOUD_REGION=$REGION,PYTHONPATH=/home/fable" \
  
echo "Deployment complete! Service URL:"
gcloud run services describe $SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --format 'value(status.url)' || handle_error

echo ""
echo "Deployment finished successfully at $(date)! Press any key to close..."
read -n 1 -s