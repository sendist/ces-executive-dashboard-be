import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from 'prisma/prisma.module';
import { BullModule } from '@nestjs/bullmq';
import { UploadModule } from './upload/upload.module';
import { DashboardModule } from './dashboard/dashboard.module';
import { AuthModule } from './modules/auth/auth.module';

@Module({
  imports: [
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

    AuthModule,
    
    PrismaModule,
    UploadModule,
    DashboardModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
