import { Module } from '@nestjs/common';
import { DashboardController } from './dashboard.controller';
import { DashboardService } from './dashboard.service';
import { PrismaService } from '../../prisma/prisma.service';
import { OcaService } from './oca.service';

@Module({
  controllers: [DashboardController],
  providers: [DashboardService, PrismaService, OcaService],
})
export class DashboardModule {}