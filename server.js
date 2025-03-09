// server.js - Express server for video processing
const express = require('express');
const { Minio } = require('minio');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);
const os = require('os');
const app = express();

// Parse JSON body
app.use(express.json());

// Configure Minio client
const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT || '',
  port: parseInt(process.env.MINIO_PORT || '433'),
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || '',
  secretKey: process.env.MINIO_SECRET_KEY || ''
});

app.post('/process-video', async (req, res) => {
  const { bucket, key, outputPrefix } = req.body;
  
  if (!bucket || !key || !outputPrefix) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }
  
  // Generate a unique job ID
  const jobId = `job_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
  
  // Start processing asynchronously
  processVideo(bucket, key, outputPrefix, jobId)
    .catch(err => console.error(`Error processing job ${jobId}:`, err));
  
  // Return immediately with the job ID
  return res.status(202).json({ 
    message: 'Video processing queued', 
    jobId,
    status: 'queued'
  });
});

async function processVideo(bucket, key, outputPrefix, jobId) {
  // Create temporary directory for processing
  const tempDir = path.join(os.tmpdir(), `video_processing_${jobId}`);
  fs.mkdirSync(tempDir, { recursive: true });
  
  const inputFile = path.join(tempDir, path.basename(key));
  const outputDir = path.join(tempDir, 'output');
  fs.mkdirSync(outputDir, { recursive: true });
  
  try {
    // Download the source video
    console.log(`Downloading ${bucket}/${key} to ${inputFile}`);
    await minioClient.fGetObject(bucket, key, inputFile);
    
    // Process the video with HSL adjustments and create HLS playlist
    console.log('Processing video with HSL adjustments');
    await execAsync(`ffmpeg -i "${inputFile}" \
      -vf "hue=h=15:s=1.2:l=1.1" \
      -c:v h264 -crf 23 -preset medium \
      -c:a aac -b:a 128k \
      -hls_time 6 \
      -hls_list_size 0 \
      -hls_segment_filename "${outputDir}/segment_%03d.ts" \
      "${outputDir}/master.m3u8"`);
    
    // List all generated files
    const files = fs.readdirSync(outputDir);
    
    // Upload all generated files to the output prefix in Minio
    console.log('Uploading processed files');
    for (const file of files) {
      const filePath = path.join(outputDir, file);
      const objectKey = `${outputPrefix}/${file}`;
      
      await minioClient.fPutObject(
        bucket,
        objectKey,
        filePath,
        { 'Content-Type': file.endsWith('.m3u8') ? 'application/x-mpegURL' : 'video/MP2T' }
      );
      
      console.log(`Uploaded ${filePath} to ${bucket}/${objectKey}`);
    }
    
  
    
  } catch (error) {
    console.error(`Error processing video:`, error);
  } finally {
    // Clean up temporary files
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch (err) {
      console.error('Error cleaning up temp directory:', err);
    }
  }
}



const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Video processing service running on port ${PORT}`);
});