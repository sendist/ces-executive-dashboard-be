// src/excel/excel.module.ts
import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { UploadController } from '../upload/upload.controller';
import { ExcelProcessor } from './excel.processor';
import { PrismaService } from '../../prisma/prisma.service'; // Assuming you have this
import { CallUploadService } from './services/call-upload.service';
import { CsatUploadService } from './services/csat-upload.service';
import { OcaUploadService } from './services/oca-upload.service';
import { OmnixUploadService } from './services/omnix-upload.service';
import { OcaUpsertService } from './repository/oca-upsert.service';
import { OcaReportSchedulerService } from './scheduler/oca-report-scheduler.service';

@Module({
  imports: [
    // Register the specific queue we used in the Controller
    BullModule.registerQueue({
      name: 'excel-queue', 
    }),
  ],
  controllers: [UploadController],
  providers: [
    ExcelProcessor, 
    CallUploadService,
    CsatUploadService,
    OcaUploadService,
    OmnixUploadService,
    PrismaService,
    OcaUpsertService,
    OcaReportSchedulerService,
  ],
  exports: [BullModule], 
})
export class ExcelModule {}