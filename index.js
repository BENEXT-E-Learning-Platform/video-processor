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
const ffmpeg = require('fluent-ffmpeg');
const { exec } = require('child_process');
const path = require('path');

async function getVideoInfo(inputPath) {
  return new Promise((resolve, reject) => {
    exec(
      `ffprobe -v error -show_entries stream=width,height -of json "${inputPath}"`,
      (error, stdout, stderr) => {
        if (error) {
          console.error('ffprobe error:', stderr);
          return reject(error);
        }
        const info = JSON.parse(stdout);
        const videoStream = info.streams.find((s) => s.width && s.height);
        resolve({
          width: videoStream?.width || 0,
          height: videoStream?.height || 0,
        });
      }
    );
  });
}

async function processVideoWithFFmpeg(inputPath, outputDir, duration) {
  const segmentDuration = duration < 300 ? 2 : 4;
  const outputMaster = path.join(outputDir, 'master.m3u8');

  return new Promise(async (resolve, reject) => {
    try {
      // Get input resolution
      const { width, height } = await getVideoInfo(inputPath);
      console.log(`Input resolution: ${width}x${height}`);

      // Define variant configurations based on input resolution
      const variants = [];
      if (height <= 360) {
        variants.push({ bitrate: '400k', resolution: '480:trunc(ow/a/2)*2' }); // Downscale or maintain
      } else if (height <= 720) {
        variants.push({ bitrate: '400k', resolution: '480:trunc(ow/a/2)*2' });
        variants.push({ bitrate: '1000k', resolution: `${Math.min(width, 1280)}:trunc(ow/a/2)*2` });
      } else {
        variants.push({ bitrate: '400k', resolution: '480:trunc(ow/a/2)*2' });
        variants.push({ bitrate: '1000k', resolution: '720:trunc(ow/a/2)*2' });
        variants.push({ bitrate: '2000k', resolution: `${Math.min(width, 1920)}:trunc(ow/a/2)*2` });
      }

      // Build FFmpeg command
      const ffmpegProcess = ffmpeg(inputPath)
        .outputOptions([
          '-preset slow',
          '-g 48',
          '-sc_threshold 0',
        ])
        .videoCodec('libx264');

      // Add video variants dynamically
      variants.forEach((variant, index) => {
        ffmpegProcess.addOutputOptions([
          '-map 0:v:0',
          `-b:v:${index} ${variant.bitrate}`,
          `-filter:v:${index} scale=${variant.resolution}`,
        ]);
      });

      // Audio configuration
      ffmpegProcess
        .audioCodec('aac')
        .audioBitrate('96k')
        .audioChannels(2)
        .addOutputOptions('-map 0:a:0?'); // Optional audio mapping

      // HLS configuration
      const varStreamMap = variants
        .map((_, index) => `v:${index}${variants.length > 1 ? ',a:0' : ''}`)
        .join(' ');
      ffmpegProcess
        .format('hls')
        .addOutputOptions([
          `-hls_time ${segmentDuration}`,
          '-hls_list_size 0',
          `-hls_segment_filename ${outputDir}/%v_segment%d.ts`,
          '-var_stream_map', varStreamMap,
          `-master_pl_name master.m3u8`,
        ])
        .output(outputMaster);

      ffmpegProcess
        .on('start', (commandLine) => console.log('Executing FFmpeg command:', commandLine))
        .on('progress', (progress) => console.log('Processing progress:', progress.percent + '%'))
        .on('end', () => {
          console.log('FFmpeg processing complete');
          resolve();
        })
        .on('error', (err, stdout, stderr) => {
          console.error('FFmpeg error:', err.message);
          console.error('FFmpeg stdout:', stdout);
          console.error('FFmpeg stderr:', stderr);
          reject(err);
        })
        .run();
    } catch (error) {
      reject(error);
    }
  });
}

// Export for use in index.js
module.exports = processVideoWithFFmpeg;

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