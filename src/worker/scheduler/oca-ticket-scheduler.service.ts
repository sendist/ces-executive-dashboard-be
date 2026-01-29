// ticket-scheduler.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import axios from 'axios';
import moment from 'moment';
import { PrismaService } from 'prisma/prisma.service';

@Injectable()
export class OcaTicketSchedulerService {
  private readonly logger = new Logger(OcaTicketSchedulerService.name);

  constructor(
    @InjectQueue('ticket-processing') private ticketQueue: Queue,
    private readonly prisma: PrismaService,
  ) {}

  // Run every 10 minutes
  @Cron(process.env.CRON_SYNC_DAILY_OCA ?? CronExpression.EVERY_30_MINUTES)
  async handleCron() {
    this.logger.debug('Starting ticket sync...');

    // 1. Determine Date Range (e.g., fetch last 24 hours to catch updates)
    // const startDate = moment().subtract(1, 'days').format('YYYY-MM-DD');
    const todayDate = moment().format('YYYY-MM-DD');

    let page = 1;
    let hasMore = true;
    let lastJob = '';

    while (hasMore) {
      // hasMore = false;
      // 2. Hit the List API
      const response = await axios.post(
        'https://webapigw.ocatelkom.co.id/oca-interaction/ticketing/get-list',
        {
          agent_id: '621464b818b240212019132c',
          application: '621463e262b3c500214ab937',
          filterOptions: [
            {
              key: 'range_date',
              values: { start_date: todayDate, end_date: todayDate },
            },
            // {
            //   key: 'status',
            //   values: ['open']
            // },
            // {
            //   key: 'channel',
            //   values: ['form']
            // }
          ],
          limit: 500, // Increase limit for batching
          page: page,
          search: {
            key: '',
            value: '',
          },
          sort: { created: -1 },
        },
      );

      const tickets = response.data.results.data;
      const ticketNumbers = tickets.map((t) => t.ticket_number);

      const dbRows = await this.prisma.$queryRaw<
        { ticket_number: string; last_update: Date | null }[]
      >`
        SELECT "ticket_number", "last_update"
        FROM "RawOca"
        WHERE "ticket_number" = ANY(${ticketNumbers});
`;
      const dbMap = new Map(
        dbRows.map((r) => [r.ticket_number, r.last_update?.getTime()]),
      );

      const ticketsToProcess = tickets.filter((t) => {
        const dbLast = dbMap.get(t.ticket_number);
        if (!dbLast) return true; // new ticket
        return new Date(t.updated_at).getTime() > dbLast;
      });

      // 3. Push to Queue

      if (ticketsToProcess.length > 0) {
        // 2. Push the WHOLE BATCH to the queue as ONE job
        // Generate a job ID based on the first+last ticket to avoid duplicates if needed
        const batchId = `batch-${page}-${ticketsToProcess[0].ticket_id}-${moment().unix()}`;

        const job = await this.ticketQueue.add(
          'process-batch-tickets', // New job name
          {
            tickets: ticketsToProcess, // Payload is an ARRAY now
          },
          { jobId: batchId },
        );

        this.logger.log(
          `Queued batch page ${page} with ${ticketsToProcess.length} tickets, jobId: ${job.id}`,
        );
        lastJob = job?.id ?? '';
      }

      // Pagination Logic
      if (page >= response.data.results.pages) {
        hasMore = false;
      } else {
        page++;
      }
    }

    // Save last sync to Postgres
    const now = new Date();
    const lastSyncWib = now
      ? moment(now).tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss')
      : null;
    await this.prisma.ocaDailySync.upsert({
      where: { id: 1 }, // Always keep one row
      update: { lastSync: now },
      create: { id: 1, lastSync: now },
    });

    this.logger.log('Ticket sync process completed.');
    return { lastJob, lastSync: lastSyncWib };
  }

  async getLastSyncTime() {
    const record = await this.prisma.ocaDailySync.findUnique({
      where: { id: 1 },
    });
    return record?.lastSync ?? null;
  }
}
