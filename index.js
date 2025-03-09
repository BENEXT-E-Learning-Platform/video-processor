const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const ffmpeg = require('fluent-ffmpeg');
const fs = require('fs').promises;
const path = require('path');
const { exec } = require('child_process');
const express = require('express');

const app = express();
app.use(express.json());

const s3Client = new S3Client({
  region: process.env.MINIO_REGION || 'us-east-1',
  endpoint: process.env.MINIO_ENDPOINT,
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.MINIO_ROOT_USER,
    secretAccessKey: process.env.MINIO_ROOT_PASSWORD,
  },
});

const processingQueue = [];
let isProcessing = false;

async function downloadFromMinIO(bucket, key, outputPath) {
  console.log(`Downloading ${key} from MinIO...`);
  const getCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3Client.send(getCommand);
  const writeStream = require('fs').createWriteStream(outputPath);
  response.Body.pipe(writeStream);
  return new Promise((resolve, reject) => {
    writeStream.on('finish', () => {
      console.log(`Download complete: ${outputPath}`);
      resolve();
    });
    writeStream.on('error', reject);
  });
}

async function getVideoDuration(inputPath) {
  return new Promise((resolve, reject) => {
    exec(`ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`, (error, stdout) => {
      if (error) return reject(error);
      resolve(parseFloat(stdout));
    });
  });
}

async function processVideoWithFFmpeg(inputPath, outputDir, duration) {
    const segmentDuration = duration < 300 ? 2 : 4;
    const outputMaster = path.join(outputDir, 'master.m3u8');
  
    return new Promise((resolve, reject) => {
      const ffmpegProcess = ffmpeg(inputPath)
        .outputOptions([
          '-preset slow',
          '-g 48',
          '-sc_threshold 0',
        ])
        // Video variants
        .videoCodec('libx264')
        .addOutputOptions([
          '-map 0:v:0', // First video stream
          '-b:v:0 400k',
          '-filter:v:0 scale=480:trunc(ow/a/2)*2',
          '-map 0:v:0', // Second video stream
          '-b:v:1 1000k',
          '-filter:v:1 scale=720:trunc(ow/a/2)*2',
          '-map 0:v:0', // Third video stream
          '-b:v:2 2000k',
          '-filter:v:2 scale=1080:trunc(ow/a/2)*2',
        ])
        // Audio (if present)
        .audioCodec('aac')
        .audioBitrate('96k')
        .audioChannels(2)
        .addOutputOptions('-map 0:a:0?') // Optional audio mapping
        // HLS settings
        .format('hls')
        .addOutputOptions([
          `-hls_time ${segmentDuration}`,
          '-hls_list_size 0',
          `-hls_segment_filename ${outputDir}/%v_segment%d.ts`,
          '-var_stream_map', 'v:0,a:0 v:1,a:0 v:2,a:0', // Split option and value
          `-master_pl_name master.m3u8`,
        ])
        .output(outputMaster);
  
      ffmpegProcess
        .on('start', (commandLine) => {
          console.log('Executing FFmpeg command:', commandLine);
        })
        .on('end', () => {
          console.log('FFmpeg processing complete');
          resolve();
        })
        .on('error', (err, stdout, stderr) => {
          console.error('FFmpeg error:', err.message);
          console.error('FFmpeg stderr:', stderr);
          reject(err);
        })
        .run();
    });
  }

async function uploadToMinIO(bucket, prefix, outputDir) {
  console.log('Uploading processed files to MinIO...');
  const files = await fs.readdir(outputDir);
  for (const file of files) {
    const filePath = path.join(outputDir, file);
    const fileContent = await fs.readFile(filePath);
    const putCommand = new PutObjectCommand({
      Bucket: bucket,
      Key: `${prefix}/${file}`,
      Body: fileContent,
    });
    await s3Client.send(putCommand);
  }
}

async function processNextInQueue() {
  if (isProcessing || processingQueue.length === 0) return;
  isProcessing = true;
  const job = processingQueue.shift();
  console.log(`Processing ${job.key} from bucket ${job.bucket} (Job ID: ${job.jobId})`);

  const tempDir = `/tmp/${job.jobId}`;
  const inputPath = path.join(tempDir, 'input.mp4');
  const outputDir = path.join(tempDir, 'output');

  try {
    await fs.mkdir(tempDir, { recursive: true });
    await fs.mkdir(outputDir, { recursive: true });
    await downloadFromMinIO(job.bucket, job.key, inputPath);
    const duration = await getVideoDuration(inputPath);
    console.log(`Video duration: ${duration} seconds`);
    console.log('Processing video with FFmpeg...');
    await processVideoWithFFmpeg(inputPath, outputDir, duration);
    await uploadToMinIO(job.bucket, job.outputPrefix, outputDir);
    console.log(`Successfully processed ${job.key} (Job ID: ${job.jobId})`);
  } catch (error) {
    console.error(`Error processing ${job.key} (Job ID: ${job.jobId}):`, error);
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
    isProcessing = false;
    processNextInQueue();
  }
}

app.post('/process-video', (req, res) => {
  const { bucket, key, outputPrefix } = req.body;
  if (!bucket || !key || !outputPrefix) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }
  const jobId = Date.now().toString();
  processingQueue.push({ bucket, key, outputPrefix, jobId });
  console.log(`Received processing request for ${key} in bucket ${bucket}`);
  res.status(202).json({ message: 'Video queued for processing', position: processingQueue.length, jobId });
  processNextInQueue();
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Video processing service listening at http://localhost:${PORT}`);
});