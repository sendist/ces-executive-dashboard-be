// src/excel/excel.module.ts
import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { UploadController } from './upload.controller';
import { PrismaService } from '../../prisma/prisma.service'; // Assuming you have this
import { ExcelProcessor } from '../utils/excel.processor';

@Module({
  imports: [
    // 1. Register the specific queue we used in the Controller
    BullModule.registerQueue({
      name: 'excel-queue', // MUST match the name used in @InjectQueue('excel-queue')
    }),
  ],
  controllers: [UploadController],
  providers: [
    PrismaService,
    ExcelProcessor,
  ],
  exports: [BullModule], // Export if other modules need to add jobs to this queue
})
export class UploadModule {}