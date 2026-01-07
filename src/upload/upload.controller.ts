import { Controller, Post, UseInterceptors, UploadedFile, Get, Param, NotFoundException } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { diskStorage } from 'multer';

@Controller('upload')
export class UploadController {
  constructor(@InjectQueue('excel-queue') private excelQueue: Queue) {}

  @Post()
  @UseInterceptors(FileInterceptor('file', {
    storage: diskStorage({ destination: './uploads' }) // Save temp file
  }))
  async uploadExcel(@UploadedFile() file: Express.Multer.File) {
    // 1. Send job to the queue immediately
    await this.excelQueue.add('process-excel', {
      path: file.path,
      filename: file.originalname,
    });

    // 2. Return success immediately (User doesn't wait)
    return { message: 'File received. Processing started.', jobId: file.filename };
  }

  @Get('status/:jobId')
  async getJobStatus(@Param('jobId') jobId: string) {
    const job = await this.excelQueue.getJob(jobId);

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    // 1. Check if job is finished
    const isCompleted = await job.isCompleted();
    const isFailed = await job.isFailed();

    if (isCompleted) {
      // 2. Return the data you returned from the processor
      return {
        status: 'completed',
        result: job.returnvalue // This is your { stats: { inserted, updated } }
      };
    }

    if (isFailed) {
      return {
        status: 'failed',
        error: job.failedReason
      };
    }

    // 3. If still running, return progress (optional)
    // You can use job.progress if you implemented updateProgress inside the processor
    return {
      status: 'active',
      progress: job.progress
    };
  }
}