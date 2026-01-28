import { Controller, Post, Get, Patch, Param, Body, ParseIntPipe } from '@nestjs/common';
import { IncidentService } from './incident.service';
import { IncidentReport } from '@prisma/client';

@Controller('incidents')
export class IncidentController {
  constructor(private readonly incidentService: IncidentService) {}

  @Post()
  async create(@Body() data: { title: string; description: string }): Promise<IncidentReport> {
    return this.incidentService.createIncident(data);
  }

  @Get('active')
  async findActive(): Promise<IncidentReport[]> {
    return this.incidentService.getActiveIncidents();
  }

  @Get('inactive')
  async findInactive(): Promise<IncidentReport[]> {
    return this.incidentService.getInactiveIncidents();
  }

  @Patch(':id/solve')
  async solve(@Param('id', ParseIntPipe) id: number): Promise<IncidentReport> {
    return this.incidentService.solveIncident(id);
  }
}