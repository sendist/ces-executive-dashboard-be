import { Injectable } from '@nestjs/common';
import { PrismaService } from 'prisma/prisma.service';
import { DashboardFilterDto, PaginationDto } from './dto/dashboard-filter.dto';
import { Prisma } from '@prisma/client';

@Injectable()
export class OcaService {
  constructor(private readonly prisma: PrismaService) {}

  // ---------------------------------------------------------
  // 1. TOP CARDS (Summary)
  // ---------------------------------------------------------
  async getExecutiveSummary(filter: DashboardFilterDto) {
    // We use a single SQL query to get all top-level metrics at once
    const result = await this.prisma.$queryRaw<any[]>`
      SELECT 
        COUNT(*)::int as "totalTickets",
        COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "totalOpen",
        COUNT(*) FILTER (WHERE "last_status" = 'Closed')::int as "totalClosed",
        COUNT(*) FILTER (WHERE "assignee" IS NOT NULL AND "assignee" != '')::int as "assigned",
        COUNT(*) FILTER (WHERE "assignee" IS NULL OR "assignee" = '')::int as "unassigned",
        
        -- SLA Calculation: (IN SLA / Valid Tickets) * 100
        CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
             THEN ROUND(
                (COUNT(*) FILTER (WHERE "sla" = 'IN SLA')::decimal / 
                 COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
             )
             ELSE 0 
        END as "slaPercentage"
      FROM "RawOca"
      WHERE "ticket_created" BETWEEN ${filter.startDate}::timestamp AND ${filter.endDate}::timestamp
    `;

    return result[0];
  }

  // ---------------------------------------------------------
  // 2. CHANNEL BREAKDOWN (The Complex Pivot)
  // ---------------------------------------------------------
  async getChannelStats(filter: DashboardFilterDto) {
    // This query calculates all your custom conditions in one pass per channel
    const stats = await this.prisma.$queryRaw<any[]>`
      SELECT 
        channel,
        COUNT(*)::int as "total",
        
        -- % SLA
        CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
             THEN ROUND((COUNT(*) FILTER (WHERE "sla" = 'IN SLA')::decimal / NULLIF(COUNT(*) FILTER (WHERE "statusTiket" = true),0)) * 100, 2)
             ELSE 0 END as "pctSla",

        -- Basic Counts
        COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "open",
        COUNT(*) FILTER (WHERE "last_status" = 'Closed')::int as "closed",

        -- Product Specifics
        COUNT(*) FILTER (WHERE "last_status" = 'Open' AND "product" = 'connectivity')::int as "connOpen",
        COUNT(*) FILTER (WHERE "last_status" = 'Open' AND "product" = 'solution')::int as "solOpen",
        
        -- Resolve Time Logic (assuming Postgres interval math)
        COUNT(*) FILTER (WHERE "product" = 'connectivity' AND ("resolve_time" - "ticket_created") > interval '3 hours')::int as "connOver3h",
        COUNT(*) FILTER (WHERE "product" = 'solution' AND ("resolve_time" - "ticket_created") > interval '6 hours')::int as "solOver6h",

        -- FCR Stats
        COUNT(*) FILTER (WHERE "fcr" = 'NON FCR')::int as "nonFcrCount",
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "fcr" = 'FCR')::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctFcr",
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "fcr" = 'NON FCR')::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctNonFcr",

        -- Pareto Stats
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "isPareto" = true)::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctPareto",
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "isPareto" = false)::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctNotPareto"

      FROM "RawOca"
      WHERE "ticket_created" BETWEEN ${filter.startDate}::timestamp AND ${filter.endDate}::timestamp
      GROUP BY "channel"
    `;

    // Fetch "Top Corporate" and "Top KIP" separately because merging them into the GroupBy above is extremely expensive and complex
    // We will map them in code (application-side join)
    const enhancedStats = await Promise.all(stats.map(async (stat) => {
        const topCorp = await this.getTopEntityForChannel(filter, stat.channel, 'nama_perusahaan');
        const topKip = await this.getTopEntityForChannel(filter, stat.channel, 'detail_category');
        return { ...stat, topCorporate: topCorp, topKip: topKip };
    }));

    return enhancedStats;
  }

  private async getTopEntityForChannel(filter: DashboardFilterDto, channel: string, column: string) {
     // Helper to find the "Mode" (most frequent item)
     // Note: We use string interpolation for column name (safe here as it's internal controlled)
     const result = await this.prisma.$queryRawUnsafe<any[]>(`
        SELECT "${column}" as name, COUNT(*)::int as total
        FROM "RawOca"
        WHERE "channel" = $1 
          -- AND "last_status" = 'Open'
          AND "ticket_created" BETWEEN $2::timestamp AND $3::timestamp
        GROUP BY "${column}"
        ORDER BY total DESC
        LIMIT 5
     `, channel, filter.startDate, filter.endDate);
     return result || { name: '-', total: 0 };
  }

  // ---------------------------------------------------------
  // 3. ESCALATION TYPE LIST (Paginated & Searchable)
  // ---------------------------------------------------------
  async getEscalationSummary(query: PaginationDto) {
    const { page, limit, search, startDate, endDate } = query;
    const skip = ((page? page : 1) - 1) * (limit? limit:10);

    // 1. Aggregates for the header (EBO, GTM, etc.)
    const summary = await this.prisma.$queryRaw<any[]>`
      SELECT 
        "eskalasi" as type,
        COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "totalOpen",
        COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h"
      FROM "RawOca"
      WHERE "eskalasi" IS NOT NULL AND "eskalasi" != ''
        AND "ticket_created" BETWEEN ${startDate}::timestamp AND ${endDate}::timestamp
      GROUP BY "eskalasi"
    `;

    // 2. Detailed List (Prisma findMany is better here for type safety & pagination)
    const whereClause: any = {
        eskalasi: { not: '' },
        ticketCreated: { gte: new Date(startDate? startDate : new Date() ), lte: new Date(endDate? endDate : new Date()) },
        OR: search ? [
            { ticketNumber: { contains: search, mode: 'insensitive' } },
            { idRemedyNo: { contains: search, mode: 'insensitive' } }
        ] : undefined
    };

    const [total, data] = await Promise.all([
        this.prisma.rawOca.count({ where: whereClause }),
        this.prisma.rawOca.findMany({
            where: whereClause,
            select: {
                ticketCreated: true,
                ticketNumber: true,
                idRemedyNo: true, // "id case"
                ticketDuration: true,
                assignee: true, // "act name"
                department: true, // "unit id"
                eskalasi: true
            },
            skip,
            take: limit,
            orderBy: { ticketCreated: 'desc' }
        })
    ]);

    return { 
        summary, 
        list: { data, total, page, limit } 
    };
  }

  // ---------------------------------------------------------
  // 4. VIP & PARETO STATS
  // ---------------------------------------------------------
  async getSpecialAccountStats(filter: DashboardFilterDto, type: 'VIP' | 'PARETO') {
    const isVip = type === 'VIP';
    // Logic: VIP is isVip=true. Pareto is isPareto=true AND isVip=false (as per req)
    const condition = isVip 
        ? `WHERE "isVip" = true` 
        : `WHERE "isPareto" = true AND "isVip" = false`;

    const rawQuery = `
        SELECT 
            COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "openTickets",
            COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h"
        FROM "RawOca"
        ${condition}
        AND "ticket_created" BETWEEN $1::timestamp AND $2::timestamp
    `;

    const stats = await this.prisma.$queryRawUnsafe<any[]>(rawQuery, filter.startDate, filter.endDate);

    // Top 10 Corp List
    const topCorps = await this.prisma.$queryRawUnsafe<any[]>(`
        SELECT "nama_perusahaan", COUNT(*)::int as total
        FROM "RawOca"
        ${condition}
        AND "ticket_created" BETWEEN $1::timestamp AND $2::timestamp
        GROUP BY "nama_perusahaan"
        ORDER BY total DESC
        LIMIT 10
    `, filter.startDate, filter.endDate);

    return { stats: stats[0], topCorps };
  }

  // ---------------------------------------------------------
  // 5. TOP 3 KIP PER COMPANY (Advanced SQL)
  // ---------------------------------------------------------
  async getTopKipPerCompany(query: PaginationDto) {
    const { page, limit, search, startDate, endDate } = query;
    const offset = ((page? page : 1) - 1) * (limit? limit:10);

    // This is hard. We need to find the Top N companies first (based on filters), 
    // then for those companies, find their Top 3 KIPs.

    // Step 1: Get paginated list of companies matching search
    const companyQuery = `
        SELECT "nama_perusahaan", COUNT(*)::int as total_tickets
        FROM "RawOca"
        WHERE "ticket_created" BETWEEN $1::timestamp AND $2::timestamp
        ${search ? `AND "nama_perusahaan" ILIKE '%' || $3 || '%'` : ''}
        GROUP BY "nama_perusahaan"
        ORDER BY total_tickets DESC
        LIMIT ${limit? limit: 10} OFFSET ${offset}
    `;
    
    // Pass params carefully based on whether search exists
    const params = search ? [startDate, endDate, search] : [startDate, endDate];
    const companies = await this.prisma.$queryRawUnsafe<any[]>(companyQuery, ...params);

    if (companies.length === 0) return { data: [], total: 0 };

    // Step 2: For these specific companies, get Top 3 KIPs
    const companyNames = companies.map(c => c.nama_perusahaan);
    
    // We use a LATERAL JOIN or Window Function in a second query filtered by these companies
    const kips = await this.prisma.$queryRaw<any[]>`
        WITH RankedKip AS (
            SELECT 
                "nama_perusahaan", 
                "detail_category", 
                COUNT(*)::int as kip_count,
                ROW_NUMBER() OVER(PARTITION BY "nama_perusahaan" ORDER BY COUNT(*) DESC)::int as rn
            FROM "RawOca"
            WHERE "nama_perusahaan" IN (${Prisma.join(companyNames)})
              AND "ticket_created" BETWEEN ${startDate}::timestamp AND ${endDate}::timestamp
            GROUP BY "nama_perusahaan", "detail_category"
        )
        SELECT * FROM RankedKip WHERE rn <= 3
    `;

    // Step 3: Map KIPs back to companies in JS
    const result = companies.map(comp => {
        return {
            company: comp.nama_perusahaan,
            totalTickets: comp.total_tickets,
            topKips: kips.filter(k => k.nama_perusahaan === comp.nama_perusahaan)
        };
    });

    return result;
  }
  
  // ---------------------------------------------------------
  // 6. PRODUCT BREAKDOWN (Connectivity, Solution, etc)
  // ---------------------------------------------------------
  async getProductBreakdown(filter: DashboardFilterDto) {
      // Metric Aggregation
      const products = await this.prisma.$queryRaw<any[]>`
        SELECT 
            "product",
            COUNT(*)::int as "total",
            COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "open",
            COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h",
             CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "sla" = 'IN SLA')::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "pctSla"
        FROM "RawOca"
        WHERE "ticket_created" BETWEEN ${filter.startDate}::timestamp AND ${filter.endDate}::timestamp
        AND "product" IN ('CONNECTIVITY', 'SOLUTION', 'DADS')
        GROUP BY "product"
      `;
      
      // Attach Top 10 KIP per product
      const detailed = await Promise.all(products.map(async (p) => {
          const topKips = await this.prisma.$queryRaw<any[]>`
            SELECT "detail_category", COUNT(*)::int as total,
            -- Recalculate SLA just for this KIP
            CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "sla" = 'IN SLA')::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "kipSla"
            FROM "RawOca"
            WHERE "product" = ${p.product}
              AND "ticket_created" BETWEEN ${filter.startDate}::timestamp AND ${filter.endDate}::timestamp
            GROUP BY "detail_category"
            ORDER BY total DESC
            LIMIT 10
          `;
          return { ...p, topKips };
      }));
      
      return detailed;
  }
}