import {
  Controller,
  Post,
  UseInterceptors,
  UploadedFile,
  Get,
  Param,
  NotFoundException,
  Body,
  BadRequestException,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { diskStorage } from 'multer';
import { OcaReportSchedulerService } from 'src/worker/scheduler/oca-report-scheduler.service';
import moment from 'moment';
import { OcaTicketSchedulerService } from 'src/worker/scheduler/oca-ticket-scheduler.service';

@Controller('schedule')
export class ScheduleController {
  constructor(
    @InjectQueue('ticket-processing') private scheduleQueue: Queue,
    private readonly ocaReportService: OcaReportSchedulerService,
    private readonly ocaTicketSchedulerService: OcaTicketSchedulerService,
  ) {}

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
        result: job.returnvalue, // This is your { stats: { inserted, updated } }
      };
    }

    if (isFailed) {
      return {
        status: 'failed',
        error: job.failedReason,
      };
    }

    // 3. If still running, return progress (optional)
    // You can use job.progress if you implemented updateProgress inside the processor
    return {
      status: 'active',
      progress: job.progress,
    };
  }

  @Post('trigger-oca-sync')
  async triggerSync(
    @Body('startDate') startDate?: string,
    @Body('endDate') endDate?: string,
  ) {
    // 1. Validation: Ensure dates are provided or use defaults
    const start =
      startDate ||
      moment().tz('Asia/Jakarta').subtract(8, 'days').format('YYYY-MM-DD');
    const end =
      endDate ||
      moment().tz('Asia/Jakarta').subtract(1, 'days').format('YYYY-MM-DD');

    // 2. Format validation (YYYY-MM-DD)
    if (
      !moment(start, 'YYYY-MM-DD', true).isValid() ||
      !moment(end, 'YYYY-MM-DD', true).isValid()
    ) {
      throw new BadRequestException('Invalid date format. Use YYYY-MM-DD');
    }

    // 3. Trigger the process (Note: This will wait for the polling to finish)
    // If you want the API to return immediately, remove the 'await'
    const result = await this.ocaReportService.processOcaReport(start, end);

    return {
      message: 'Manual OCA Sync started and queued.',
      ...result,
    };
  }

  @Post('sync-daily-oca')
  async syncDailyOca() {
    const { lastJob, lastSync } = await this.ocaTicketSchedulerService.handleCron();

    return {
      message: 'All ticket batches have been queued.',
      jobId: lastJob,
      lastSync: lastSync,
    };
  }

  @Get('last-sync')
  async getLastSync() {
    const lastSyncUtc = await this.ocaTicketSchedulerService.getLastSyncTime();
    const lastSyncWib = lastSyncUtc
      ? moment(lastSyncUtc).tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss')
      : null;
    return { lastSyncWib };
  }
}
