// index.js
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
  region: process.env.MINIO_REGION || 'us-east-1',
  endpoint: process.env.MINIO_ENDPOINT,
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.MINIO_ROOT_USER,
    secretAccessKey: process.env.MINIO_ROOT_PASSWORD
  }
});

// Simple queue management (will be replaced by Redis later)
const processingQueue = [];
let isProcessing = false;

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'ok',
    queueLength: processingQueue.length,
    isProcessing
  });
});

// Process video endpoint
app.post('/process-video', express.json(), async (req, res) => {
  const { bucket, key, outputPrefix } = req.body;
  
  if (!bucket || !key) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }
  
  console.log(`Received processing request for ${key} in bucket ${bucket}`);
  
  // Add to queue
  const jobId = Date.now().toString();
  processingQueue.push({
    id: jobId,
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
    jobId
  });
});

// Get job status endpoint
app.get('/job/:jobId', (req, res) => {
  const { jobId } = req.params;
  
  // Check if job is in queue
  const queuePosition = processingQueue.findIndex(job => job.id === jobId);
  if (queuePosition >= 0) {
    return res.status(200).json({
      status: 'queued',
      position: queuePosition + 1
    });
  }
  
  // If not in queue and not processing, it must be complete or not found
  res.status(200).json({
    status: queuePosition === -1 ? 'completed' : 'not_found'
  });
});

async function processNextInQueue() {
  if (processingQueue.length === 0) {
    isProcessing = false;
    return;
  }
  
  isProcessing = true;
  const job = processingQueue.shift();
  console.log(`Processing ${job.key} from bucket ${job.bucket} (Job ID: ${job.id})`);
  
  try {
    // Create temporary directories
    const tempDir = `/tmp/${job.id}`;
    const tempInputPath = `${tempDir}/input${path.extname(job.key)}`;
    const tempOutputDir = `${tempDir}/output`;
    
    fs.mkdirSync(tempDir, { recursive: true });
    fs.mkdirSync(tempOutputDir, { recursive: true });
    
    // Download from MinIO
    console.log(`Downloading ${job.key} from MinIO...`);
    await downloadFromMinIO(job.bucket, job.key, tempInputPath);
    
    // Get video duration
    const duration = await getVideoDuration(tempInputPath);
    console.log(`Video duration: ${duration} seconds`);
    
    // Process with FFmpeg
    console.log(`Processing video with FFmpeg...`);
    await processVideoWithFFmpeg(tempInputPath, tempOutputDir, duration);
    
    // Upload results back to MinIO
    console.log(`Uploading processed files to MinIO...`);
    await uploadProcessedFilesToMinIO(tempOutputDir, job.bucket, job.outputPrefix);
    
    // Clean up
    fs.rmSync(tempDir, { recursive: true, force: true });
    
    console.log(`Successfully processed ${job.key} (Job ID: ${job.id})`);
  } catch (error) {
    console.error(`Error processing ${job.key} (Job ID: ${job.id}):`, error);
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

function getVideoDuration(filePath) {
  return new Promise((resolve, reject) => {
    const ffprobeCommand = `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${filePath}"`;
    
    exec(ffprobeCommand, (error, stdout, stderr) => {
      if (error) {
        console.error(`FFprobe error: ${error.message}`);
        return reject(error);
      }
      
      const duration = parseFloat(stdout.trim());
      resolve(duration);
    });
  });
}

async function processVideoWithFFmpeg(inputPath, outputDir, duration) {
    return new Promise((resolve, reject) => {
      const segmentDuration = duration < 300 ? 2 : 4;
  
      // Check if audio exists using ffprobe
      const ffprobeCommand = `ffprobe -v error -show_streams -select_streams a -of json "${inputPath}"`;
      exec(ffprobeCommand, (error, stdout) => {
        const hasAudio = !error && JSON.parse(stdout).streams.length > 0;
  
        let ffmpegCommand = `ffmpeg -i "${inputPath}" \
          -preset slow -g 48 -sc_threshold 0 \
          -map 0:v:0 -map 0:v:0 -map 0:v:0 \
          -b:v:0 400k -c:v:0 libx264 -filter:v:0 "scale=480:trunc(ow/a/2)*2" \
          -b:v:1 1000k -c:v:1 libx264 -filter:v:1 "scale=720:trunc(ow/a/2)*2" \
          -b:v:2 2000k -c:v:2 libx264 -filter:v:2 "scale=1080:trunc(ow/a/2)*2"`;
  
        if (hasAudio) {
          ffmpegCommand += ` \
            -map 0:a:0? \
            -c:a aac -b:a 96k -ac 2 \
            -var_stream_map "v:0,a:0 v:1,a:0 v:2,a:0"`;
        } else {
          ffmpegCommand += ` \
            -var_stream_map "v:0 v:1 v:2"`;
        }
  
        ffmpegCommand += ` \
          -master_pl_name master.m3u8 \
          -f hls -hls_time ${segmentDuration} -hls_list_size 0 \
          -hls_segment_filename "${outputDir}/%v_segment%d.ts" \
          "${outputDir}/%v.m3u8"`;
  
        console.log('Executing FFmpeg command:', ffmpegCommand);
  
        exec(ffmpegCommand, (error, stdout, stderr) => {
          if (error) {
            console.error('FFmpeg error:', error.message);
            console.error('FFmpeg stderr:', stderr);
            return reject(error);
          }
          console.log('FFmpeg processing complete');
          resolve();
        });
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
      contentType = 'application/vnd.apple.mpegurl';
    } else if (file.endsWith('.ts')) {
      contentType = 'video/mp2t';
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