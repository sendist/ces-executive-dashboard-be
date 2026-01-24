import { Controller, Post, UseInterceptors, UploadedFile, Get, Param, NotFoundException } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { diskStorage } from 'multer';

@Controller('schedule')
export class ScheduleController {
  constructor(@InjectQueue('ticket-processing') private scheduleQueue: Queue) {}

  @Get('status/:jobId')
  async getJobStatus(@Param('jobId') jobId: string) {
    const job = await this.scheduleQueue.getJob(jobId);

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