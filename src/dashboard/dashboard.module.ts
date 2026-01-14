import { Module } from '@nestjs/common';
import { DashboardController } from './dashboard.controller';
import { DashboardService } from './dashboard.service';
import { PrismaService } from '../../prisma/prisma.service';
import { OcaService } from './oca.service';
import { OcaOmnixService } from './oca-omnix.service';

@Module({
  controllers: [DashboardController],
  providers: [DashboardService, PrismaService, OcaService, OcaOmnixService],
})
export class DashboardModule {}