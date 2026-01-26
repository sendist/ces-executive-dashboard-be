// src/excel/excel.module.ts
import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ScheduleController } from './scheduler.controller';
import { PrismaService } from '../../prisma/prisma.service'; // Assuming you have this
import { ExcelModule } from 'src/worker/excel.module';
import { OcaTicketSchedulerService } from 'src/worker/scheduler/oca-ticket-scheduler.service';
import { DailyOcaTicketProcessor } from 'src/worker/processor/daily-oca-ticket-processor';
import { OcaUpsertService } from 'src/worker/repository/oca-upsert.service';
import { OcaReportSchedulerService } from 'src/worker/scheduler/oca-report-scheduler.service';

@Module({
  imports: [
    BullModule.registerQueue({
      name: 'ticket-processing', 
      // limiter: {
      //   max: 5,        // Limit to 5 requests
      //   duration: 1000 // per 1 second (Rate limiting the API)
      // }
    }),
    ExcelModule,
  ],
  controllers: [ScheduleController],
  providers: [
    PrismaService,
    OcaTicketSchedulerService,
    DailyOcaTicketProcessor,
    OcaUpsertService,
    OcaReportSchedulerService,
  ],
  exports: [BullModule], // Export if other modules need to add jobs to this queue
})
export class SchedulerModule {}