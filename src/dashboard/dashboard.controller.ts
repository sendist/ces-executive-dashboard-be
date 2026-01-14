import { Controller, Get, Query, ValidationPipe } from '@nestjs/common';
import { DashboardService } from './dashboard.service';
import { DashboardFilterDto, PaginationDto } from './dto/dashboard-filter.dto';
import { OcaService } from './oca.service';
import { OcaOmnixService } from './oca-omnix.service';

@Controller('dashboard')
export class DashboardController {
  constructor(
    private readonly dashboardService: DashboardService,
    private readonly ocaService: OcaOmnixService,
  ) {}

//   // Endpoint 1: Get the daily breakdown (for charts)
//   // Usage: GET /dashboard/trend?startDate=2025-01-01&endDate=2025-01-31
//   @Get('trend')
//   async getDailyTrend(@Query(new ValidationPipe({ transform: true })) filter: DashboardFilterDto) {
//     return this.dashboardService.getDailyTrend(filter);
//   }

  // Endpoint 2: Get the single summary block (aggregated math)
  // Usage: GET /dashboard/summary?startDate=2025-01-01&endDate=2025-01-31
  @Get('summarycsat')
  async getSummaryDashboard(@Query(new ValidationPipe({ transform: true })) filter: DashboardFilterDto) {
    return this.dashboardService.getSummary(filter);
  }


  @Get('summary')
  getSummary(@Query() filter: DashboardFilterDto) {
    return this.ocaService.getExecutiveSummary(filter);
  }

  @Get('channels')
  getChannels(@Query() filter: DashboardFilterDto) {
    return this.ocaService.getChannelStats(filter);
  }

  @Get('escalations')
  getEscalations(@Query() filter: PaginationDto) {
    return this.ocaService.getEscalationSummary(filter);
  }

  @Get('vip-pareto')
  async getVipPareto(@Query() filter: DashboardFilterDto) {
    const [vip, pareto] = await Promise.all([
        this.ocaService.getSpecialAccountStats(filter, 'VIP'),
        this.ocaService.getSpecialAccountStats(filter, 'PARETO')
    ]);
    return { vip, pareto };
  }
  
  @Get('company-kips')
  getCompanyKips(@Query() filter: PaginationDto) {
      return this.ocaService.getTopKipPerCompany(filter);
  }
  
  @Get('products')
  getProducts(@Query() filter: DashboardFilterDto) {
      return this.ocaService.getProductBreakdown(filter);
  }
}