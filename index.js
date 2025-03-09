const express = require('express');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const app = express();
require('dotenv').config();

const port = process.env.PORT || 3001;

// Configure MinIO client
const s3Client = new S3Client({
  region: 'us-east-1', // MinIO default
  endpoint: process.env.MINIO_ENDPOINT,
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.MINIO_ACCESS_KEY,
    secretAccessKey: process.env.MINIO_SECRET_KEY
  }
});

// Simple queue management (will be replaced by Redis later)
const processingQueue = [];
let isProcessing = false;

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

// Process video endpoint
app.post('/process-video', express.json(), async (req, res) => {
  const { bucket, key, outputPrefix } = req.body;
  
  if (!bucket || !key) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }
  
  console.log(`Received processing request for ${key} in bucket ${bucket}`);
  
  // Add to queue
  processingQueue.push({
    bucket,
    key,
    outputPrefix: outputPrefix || `processed/${path.basename(key, path.extname(key))}`
  });
  
  // Start processing if not already running
  if (!isProcessing) {
    processNextInQueue();
  }
  
  res.status(202).json({ 
    message: 'Video queued for processing',
    position: processingQueue.length,
    jobId: Date.now().toString()
  });
});

// Get processing status endpoint
app.get('/status', (req, res) => {
  res.status(200).json({
    queueLength: processingQueue.length,
    isProcessing
  });
});

async function processNextInQueue() {
  if (processingQueue.length === 0) {
    isProcessing = false;
    return;
  }
  
  isProcessing = true;
  const job = processingQueue.shift();
  console.log(`Processing ${job.key} from bucket ${job.bucket}`);
  
  try {
    // Create temporary directories
    const tempDir = `/tmp/${Date.now()}`;
    const tempInputPath = `${tempDir}/input${path.extname(job.key)}`;
    const tempOutputDir = `${tempDir}/output`;
    
    fs.mkdirSync(tempDir, { recursive: true });
    fs.mkdirSync(tempOutputDir, { recursive: true });
    
    // Download from MinIO
    console.log(`Downloading ${job.key} from MinIO...`);
    await downloadFromMinIO(job.bucket, job.key, tempInputPath);
    
    // Process with FFmpeg
    console.log(`Processing video with FFmpeg...`);
    await processVideoWithFFmpeg(tempInputPath, tempOutputDir);
    
    // Upload results back to MinIO
    console.log(`Uploading processed files to MinIO...`);
    await uploadProcessedFilesToMinIO(tempOutputDir, job.bucket, job.outputPrefix);
    
    // Clean up
    fs.rmSync(tempDir, { recursive: true, force: true });
    
    console.log(`Successfully processed ${job.key}`);
  } catch (error) {
    console.error(`Error processing ${job.key}:`, error);
  }
  
  // Process next job
  processNextInQueue();
}

async function downloadFromMinIO(bucket, key, outputPath) {
  const getCommand = new GetObjectCommand({
    Bucket: bucket,
    Key: key
  });
  
  const response = await s3Client.send(getCommand);
  const writeStream = fs.createWriteStream(outputPath);
  
  return new Promise((resolve, reject) => {
    response.Body.pipe(writeStream)
      .on('error', (err) => {
        console.error('Error downloading from MinIO:', err);
        reject(err);
      })
      .on('finish', () => {
        console.log(`Download complete: ${outputPath}`);
        resolve();
      });
  });
}

function processVideoWithFFmpeg(inputPath, outputDir) {
  return new Promise((resolve, reject) => {
    // Create HLS with multiple quality levels and encryption
    const ffmpegCommand = `ffmpeg -i "${inputPath}" \
      -preset slow -g 48 -sc_threshold 0 \
      -map 0:v:0 -map 0:a:0? -map 0:v:0 -map 0:a:0? -map 0:v:0 -map 0:a:0? \
      -b:v:0 700k -c:v:0 libx264 -filter:v:0 "scale=640:360" \
      -b:v:1 1500k -c:v:1 libx264 -filter:v:1 "scale=1280:720" \
      -b:v:2 2800k -c:v:2 libx264 -filter:v:2 "scale=1920:1080" \
      -c:a aac -b:a 128k -ac 2 \
      -var_stream_map "v:0,a:0 v:1,a:1 v:2,a:2" \
      -master_pl_name master.m3u8 \
      -f hls -hls_time 4 -hls_list_size 0 \
      -hls_segment_filename "${outputDir}/%v_segment%d.ts" \
      "${outputDir}/%v.m3u8"`;
    
    console.log('Executing FFmpeg command...');
    
    exec(ffmpegCommand, (error, stdout, stderr) => {
      if (error) {
        console.error(`FFmpeg error: ${error.message}`);
        console.error(`FFmpeg stderr: ${stderr}`);
        return reject(error);
      }
      
      console.log('FFmpeg processing complete');
      resolve();
    });
  });
}

async function uploadProcessedFilesToMinIO(directory, bucket, prefix) {
  const files = fs.readdirSync(directory);
  console.log(`Uploading ${files.length} files to MinIO...`);
  
  for (const file of files) {
    const filePath = path.join(directory, file);
    const key = `${prefix}/${file}`;
    
    // Determine content type
    let contentType = 'application/octet-stream';
    if (file.endsWith('.m3u8')) {
      contentType = 'application/x-mpegURL';
    } else if (file.endsWith('.ts')) {
      contentType = 'video/MP2T';
    }
    
    // Upload the file
    const fileContent = fs.readFileSync(filePath);
    const putCommand = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fileContent,
      ContentType: contentType
    });
    
    await s3Client.send(putCommand);
    console.log(`Uploaded ${key} to MinIO`);
  }
}

app.listen(port, () => {
  console.log(`Video processing service listening at http://localhost:${port}`);
});