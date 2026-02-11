import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import axios from 'axios';
import * as fs from 'fs';
import * as path from 'path';
import moment from 'moment-timezone'; // Highly recommended for WIB handling

@Injectable()
export class CsatReportSchedulerService {
  private readonly logger = new Logger(CsatReportSchedulerService.name);

  constructor(@InjectQueue('excel-queue') private excelQueue: Queue) {}

  // Run at 02:00 AM WIB (Asia/Jakarta)
  @Cron('01 14 * * *', { timeZone: 'Asia/Jakarta' })
  async handleScheduledReport() {
    this.logger.log('Starting scheduled CSAT Report process...');

    // 1. Calculate Date Ranges (Today-1 and 7 days before)
    const todayDate = moment().tz('Asia/Jakarta').format('YYYY-MM-DD');

    return this.processCsatReport(todayDate);
  }

  async processCsatReport(todayDate: string) {
    this.logger.log(`Processing CSAT Report for date ${todayDate}...`);
    try {
      // 2. Request Report Generation
      const documentId = await this.requestReportGeneration(todayDate);
      console.log(documentId);

      // 3. Poll for Download URL (The API is async)
      const downloadUrl = await this.pollForDownloadUrl(documentId);
      console.log(downloadUrl);

      // 4. Download file to local storage
      const filePath = await this.downloadFile(downloadUrl);

      // 5. Add to your existing BullMQ Queue
      const job = await this.excelQueue.add('process-csat-report', {
        path: filePath,
        filename: path.basename(filePath),
      });
      this.logger.log(`Successfully queued report for processing: ${filePath}`);

      return { success: true, jobId: job.id, filePath };
    } catch (error) {
      this.logger.error('Failed to process scheduled CSAT report', error.stack);
    }
    ``;
  }

  private async requestReportGeneration(todayDate: string): Promise<string> {
    const response = await axios.post(
      'https://webapigw.ocatelkom.co.id/oca-interaction/survey/generate-report',
      {
        agent_id: '621464b818b240212019132c',
        survey_id: '66f525f2075a160011713e5e',
        start_date: todayDate,
        end_date: todayDate,
      },
      {
        auth: {
          username: 'tsel-app-connectivity',
          password: '@tsel198xMu918230pp',
        },
      },
    );

    if (!response.data.status) throw new Error('CSAT Report Request Failed');
    return response.data.results.report_id;
  }

  private async pollForDownloadUrl(docId: string): Promise<string> {
    const maxRetries = 20;
    const initialDelay = 5000; // 5 seconds mandatory wait
    const delay = 30000; // 30 seconds

    this.logger.log(
      `Report requested. Waiting ${initialDelay / 1000}s for initialization...`,
    );
    await new Promise((res) => setTimeout(res, initialDelay));
    for (let i = 0; i < maxRetries; i++) {
      try {
        this.logger.log(
          `Checking report status (Attempt ${i + 1}/${maxRetries})...`,
        );

        const response = await axios.get(
          `https://webapigw.ocatelkom.co.id/oca-interaction/survey/get-report?report_id=${docId}`,
          {
            auth: {
              username: 'tsel-app-connectivity',
              password: '@tsel198xMu918230pp',
            },
          },
        );

        // If Axios gets here, it's a 200 OK
        if (response.data?.status && response.data?.results) {
          return response.data.results;
        }
      } catch (error) {
        // 1. Check if it's the expected 404 "Processing" state
        const errorData = error.response?.data;

        if (
          error.response?.status === 404 &&
          errorData?.errors?.[0]?.code === '33'
        ) {
          this.logger.warn(
            `Report ${docId} is still generating. Waiting 30s...`,
          );
        }
        // 2. Check if it's a 404 but the ID is just totally unknown yet
        else if (error.response?.status === 404 || error.response?.status === 406) {
          this.logger.warn(`Document ID not recognized yet. Retrying...`);
        }
        // 3. It's a real error (401 Unauthorized, 500 Server Error, etc.)
        else {
          this.logger.error(`Critical API Error: ${error.message}`);
          throw error;
        }
      }

      // Wait before the next loop
      await new Promise((res) => setTimeout(res, delay));
    }

    throw new Error(
      'TIMEOUT: CSAT Report generation took longer than 10 minutes.',
    );
  }

  private async downloadFile(url: string): Promise<string> {
    const fileName = `csat_report_${Date.now()}.csv`;
    const destination = path.resolve('./uploads', fileName);

    const response = await axios({
      method: 'GET',
      url: url,
      responseType: 'stream',
    });

    const writer = fs.createWriteStream(destination);
    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', () => resolve(destination));
      writer.on('error', reject);
    });
  }
}
