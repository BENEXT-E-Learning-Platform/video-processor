# Video Processing Service

A Node.js service that processes videos for e-learning platform using FFmpeg.

## Features

- Video processing with FFmpeg
- HLS adaptive bitrate streaming
- Integration with MinIO for storage
- Simple queue management

## Environment Variables

- `PORT`: Port number (default: 3001)
- `MINIO_ENDPOINT`: MinIO server URL
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_BUCKET_NAME`: MinIO bucket name

## API Endpoints

- `POST /process-video`: Queue a video for processing
- `GET /status`: Check processing queue status
- `GET /health`: Health check endpoint

## Deployment

This service is designed to be deployed on Railway.