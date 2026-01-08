// src/excel/excel.module.ts
import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { UploadController } from '../upload/upload.controller';
import { ExcelProcessor } from './excel.processor';
import { PrismaService } from '../../prisma/prisma.service'; // Assuming you have this

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
    PrismaService,
  ],
  exports: [BullModule], 
})
export class ExcelModule {}