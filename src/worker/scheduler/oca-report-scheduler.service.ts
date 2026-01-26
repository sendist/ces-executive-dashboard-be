import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import axios from 'axios';
import * as fs from 'fs';
import * as path from 'path';
import moment from 'moment-timezone'; // Highly recommended for WIB handling

@Injectable()
export class OcaReportSchedulerService {
  private readonly logger = new Logger(OcaReportSchedulerService.name);

  constructor(@InjectQueue('excel-queue') private excelQueue: Queue) {}

  // Run at 02:00 AM WIB (Asia/Jakarta)
  @Cron('0 2 * * *', { timeZone: 'Asia/Jakarta' })
  async handleScheduledReport() {
    this.logger.log('Starting scheduled OCA Report process...');

    // 1. Calculate Date Ranges (Today-1 and 7 days before)
    const endDate = moment()
      .tz('Asia/Jakarta')
      .subtract(1, 'days')
      .format('YYYY-MM-DD');
    const startDate = moment()
      .tz('Asia/Jakarta')
      .subtract(8, 'days')
      .format('YYYY-MM-DD');

    return this.processOcaReport(startDate, endDate);
  }

  async processOcaReport(startDate: string, endDate: string) {
    this.logger.log(`Processing OCA Report from ${startDate} to ${endDate}...`);
    try {
      // 2. Request Report Generation
      const documentId = await this.requestReportGeneration(startDate, endDate);

      // 3. Poll for Download URL (The API is async)
      const downloadUrl = await this.pollForDownloadUrl(documentId);

      // 4. Download file to local storage
      const filePath = await this.downloadFile(downloadUrl);

      // 5. Add to your existing BullMQ Queue
      const job = await this.excelQueue.add('process-oca-report', {
        path: filePath,
        filename: path.basename(filePath),
      });
      
      return { success: true, jobId: job.id, filePath };
      this.logger.log(`Successfully queued report for processing: ${filePath}`);
    } catch (error) {
      this.logger.error('Failed to process scheduled OCA report', error.stack);
    }
  }

  private async requestReportGeneration(
    start: string,
    end: string,
  ): Promise<string> {
    const response = await axios.post(
      'https://webapigw.ocatelkom.co.id/oca-interaction/ticketing/request_report',
      {
        agents: [],
        agent_supervisor: '621464b818b240212019132c',
        category_id: [],
        start_date: start,
        end_date: end,
        priority: [],
        source: [],
        type: 'csv',
        department_id: [],
        status: [],
        header_default: [
          'No.',
          'Ticket Number',
          'Ticket Subject',
          'Channel',
          'Category',
          'Reporter',
          'Assignee',
          'Department',
          'Priority',
          'Last Status',
          'Ticket Created',
          'Last Update',
          'Description',
          'Customer Name',
          'Customer Phone',
          'Customer Address',
          'Customer Email',
          'First Response Time',
          'Total Response Time',
          'Total Resolution Time',
          'Resolve Time',
          'Resolved By',
          'Closed Time',
          'Ticket Duration',
          'Count Inbound Message',
          'Label In Room',
          'First Response Duration',
          'Escalate Ticket',
          'Last Assignee Escalation',
          'Last Status Escalation',
          'Last Update Escalation',
        ],
        header_sub_category: [
          'Sub Category',
          'Detail Category',
          'IOT',
          'Amount Revenue',
          'Jumlah MSISDN',
          'Tags',
          'ID Remedy_NO',
          'Eskalasi/ID Remedy_IT/AO/EMS',
          'Reason OSL',
          'Project ID',
          'Nama Perusahaan',
          'Roaming',
        ],
        model_data: 'ticket_report',
      },
      {
        auth: {
          username: 'tsel-app-connectivity',
          password: '@tsel198xMu918230pp',
        },
      },
    );

    if (!response.data.status) throw new Error('OCA Report Request Failed');
    return response.data.results.document_id;
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
          `https://webapigw.ocatelkom.co.id/tsel/download-ticket-report/${docId}`,
        );

        // If Axios gets here, it's a 200 OK
        if (response.data?.status && response.data?.results?.url) {
          return response.data.results.url;
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
        else if (error.response?.status === 404) {
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
      'TIMEOUT: OCA Report generation took longer than 10 minutes.',
    );
  }

  private async downloadFile(url: string): Promise<string> {
    const fileName = `oca_report_${Date.now()}.csv`;
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
