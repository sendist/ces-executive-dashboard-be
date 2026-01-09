import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { PrismaService } from '../../prisma/prisma.service';
import { CsatUploadService } from './services/csat-upload.service';
import { CallUploadService } from './services/call-upload.service';
import { OmnixUploadService } from './services/omnix-upload.service';
import { OcaUploadService } from './services/oca-upload.service';
import * as fs from 'fs'; 

@Processor('excel-queue')
export class ExcelProcessor extends WorkerHost {
  constructor(
    private prisma: PrismaService,
    private readonly csatUploadService: CsatUploadService,
    private readonly callUploadService: CallUploadService,
    private readonly omnixUploadService: OmnixUploadService,
    private readonly ocaUploadService: OcaUploadService,
  ) {
    super();
  }

  // Traffic Controller
  async process(job: Job<any, any, string>): Promise<any> {
    const filePath = job.data.path;

    try {
      switch (job.name) {
        case 'process-csat-report':
          return await this.csatUploadService.process(job);
        case 'process-omnix-report':
          return await this.omnixUploadService.process(job);
        case 'process-call-report':
          return await this.callUploadService.process(job);
        case 'process-oca-report':
          return await this.ocaUploadService.process(job);
        default:
          throw new Error(`Unknown job name: ${job.name}`);
      }
    } catch (error) {
      console.error(`Error processing job ${job.id} (${job.name}):`, error);
      throw error;
    } finally {
      await this.removeFile(filePath);
    }
  }

  // Helper to safely delete file
  private async removeFile(filePath: string) {
    try {
      if (fs.existsSync(filePath)) {
        await fs.promises.unlink(filePath);
        // console.log(`Deleted temp file: ${filePath}`);
      }
    } catch (err) {
      console.error(`Failed to delete file ${filePath}:`, err);
    }
  }
}