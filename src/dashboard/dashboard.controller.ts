import { Controller, Get, Query, ValidationPipe } from '@nestjs/common';
import { DashboardService } from './dashboard.service';
import { DashboardFilterDto } from './dto/dashboard-filter.dto';

@Controller('dashboard')
export class DashboardController {
  constructor(private readonly dashboardService: DashboardService) {}

//   // Endpoint 1: Get the daily breakdown (for charts)
//   // Usage: GET /dashboard/trend?startDate=2025-01-01&endDate=2025-01-31
//   @Get('trend')
//   async getDailyTrend(@Query(new ValidationPipe({ transform: true })) filter: DashboardFilterDto) {
//     return this.dashboardService.getDailyTrend(filter);
//   }

  // Endpoint 2: Get the single summary block (aggregated math)
  // Usage: GET /dashboard/summary?startDate=2025-01-01&endDate=2025-01-31
  @Get('summary')
  async getSummary(@Query(new ValidationPipe({ transform: true })) filter: DashboardFilterDto) {
    return this.dashboardService.getSummary(filter);
  }
}