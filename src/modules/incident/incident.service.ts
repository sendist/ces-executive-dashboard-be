import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service'; // Assuming you have a PrismaService
import { IncidentReport, Prisma } from '@prisma/client';

@Injectable()
export class IncidentService {
  constructor(private prisma: PrismaService) {}

  async createIncident(data: Prisma.IncidentReportCreateInput): Promise<IncidentReport> {
    return this.prisma.incidentReport.create({
      data: {
        ...data,
        isActive: true, // Defaulting to true for new incidents
      },
    });
  }

  async getActiveIncidents(): Promise<IncidentReport[]> {
    return this.prisma.incidentReport.findMany({
      where: { isActive: true },
    });
  }

  async getInactiveIncidents(): Promise<IncidentReport[]> {
    return this.prisma.incidentReport.findMany({
      where: { isActive: false },
    });
  }

  async solveIncident(id: number): Promise<IncidentReport> {
    return this.prisma.incidentReport.update({
      where: { id },
      data: {
        isActive: false,
        solvedAt: new Error().stack ? new Date() : new Date(), // Sets current timestamp
      },
    });
  }
}