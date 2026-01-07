import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { DashboardFilterDto } from './dto/dashboard-filter.dto';
import { Prisma } from '@prisma/client';

@Injectable()
export class DashboardService {
  constructor(private prisma: PrismaService) {}

  async getSummary(filter: DashboardFilterDto) {
    // 1. Build the dynamic filter based on your rules
    const where = this.buildDateFilter(filter);

    // 2. Perform the aggregation
    // If only startDate is sent, this sums only that 1 day (effectively returning that day's data)
    // If range is sent, this sums everything in between
    const aggregations = await this.prisma.dailyCsatStat.aggregate({
      where,
      _sum: {
        totalSurvey: true,
        totalDijawab: true,
        totalJawaban45: true,
      },
    });

    const totalDijawab = aggregations._sum.totalDijawab || 0;
    const totalJawaban45 = aggregations._sum.totalJawaban45 || 0;

    // 3. Re-calculate Percentage
    // This logic works for both Single Day (10/100 = 10%) and Range (Sum/Sum = Average%)
    const reCalculatedCsat = totalDijawab > 0 
      ? (totalJawaban45 / totalDijawab) * 100 
      : 0;

    // 4. Handle Average Score
    // We query the RAW table to get the precise average for the selected period
    const scoreAgg = await this.prisma.rawCsat.aggregate({
      where: {
        createdAt: this.buildRawDateFilter(filter), // Use helper for Raw table
        numeric: { not: null }
      },
      _avg: { numeric: true }
    });

    return {
      totalSurvey: aggregations._sum.totalSurvey || 0,
      totalDijawab: totalDijawab,
      totalJawaban45: totalJawaban45,
      persenCsat: parseFloat(reCalculatedCsat.toFixed(2)),
      scoreCsat: parseFloat((scoreAgg._avg.numeric || 0).toFixed(2))
    };
  }

  // --- PRIVATE HELPERS ---

  /**
   * Logic for the Aggregate Table (DailyCsatStat)
   * This table has a 'date' column normalized to midnight (00:00:00)
   */
  private buildDateFilter(filter: DashboardFilterDto): Prisma.DailyCsatStatWhereInput {
    const { startDate, endDate } = filter;

    // SCENARIO 1: Only Start Date -> Exact Match for that day
    if (startDate && !endDate) {
      return {
        date: {
          equals: new Date(startDate) 
        }
      };
    }

    // SCENARIO 2: Both Dates -> Range calculation
    if (startDate && endDate) {
      return {
        date: {
          gte: new Date(startDate),
          lte: new Date(endDate),
        },
      };
    }

    return {}; // Default (All time)
  }

  /**
   * Logic for the Raw Table (RawSurveyLog)
   * This table uses 'createdAt' which has precise timestamps (e.g. 14:30:05)
   */
  private buildRawDateFilter(filter: DashboardFilterDto): Prisma.DateTimeFilter | undefined  {
    const { startDate, endDate } = filter;

    if (startDate && !endDate) {
      // For Raw Data, "One Day" means 00:00:00 to 23:59:59
      const start = new Date(startDate);
      const end = new Date(startDate);
      end.setHours(23, 59, 59, 999); // End of that day

      return {
        gte: start,
        lte: end
      };
    }

    if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999); // Include the full end date

      return {
        gte: start,
        lte: end
      };
    }

    return undefined;
  }
}