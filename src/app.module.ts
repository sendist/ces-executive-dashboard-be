import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from 'prisma/prisma.module';
import { BullModule } from '@nestjs/bullmq';
import { UploadModule } from './upload/upload.module';
import { DashboardModule } from './dashboard/dashboard.module';
import { AuthModule } from './modules/auth/auth.module';
import { ScheduleModule } from '@nestjs/schedule';
import { HttpModule } from '@nestjs/axios';
import { OcaTicketSchedulerService } from './worker/scheduler/oca-ticket-scheduler.service';
import { DailyOcaTicketProcessor } from './worker/processor/daily-oca-ticket-processor';
import { OcaUpsertService } from './worker/repository/oca-upsert.service';
import { SchedulerModule } from './scheduler/scheduler.module';
import { IncidentModule } from './modules/incident/incident.module';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';
import { NewsModule } from './modules/news/news.module';

@Module({
  imports: [
    // 1. Enable Scheduling
    ScheduleModule.forRoot(),

    BullModule.forRoot({
      connection: process.env.REDIS_URL
        ? {
            url: process.env.REDIS_URL,
            maxRetriesPerRequest: null,
            enableReadyCheck: false,
          }
        : {
            host: process.env.REDIS_HOST,
            port: Number(process.env.REDIS_PORT),
          },
    }),
    HttpModule,
    AuthModule,
    PrismaModule,
    UploadModule,
    DashboardModule,
    SchedulerModule,
    IncidentModule,
    NewsModule,
    ServeStaticModule.forRoot({
      rootPath: join(process.cwd(), 'uploads'),
      serveRoot: '/uploads',
      serveStaticOptions: {
        setHeaders: (res) => {
          // This tells the browser that this specific resource can be loaded by other origins
          res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
          // Optional: Helps with some modern browser "Isolation" security policies
          res.setHeader('Access-Control-Allow-Origin', process.env.FRONTEND_URL);
        },
      },
    }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
