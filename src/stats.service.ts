import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class StatsService {
  constructor(private prisma: PrismaService) {} 

  async getDashboardStats() {
    return this.prisma.dailyCsatStat.findMany({
      orderBy: { date: 'desc' },
      take: 30, 
    });
  }
}