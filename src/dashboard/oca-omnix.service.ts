import { Injectable } from '@nestjs/common';
import { PrismaService } from 'prisma/prisma.service';
import { DashboardFilterDto, PaginationDto } from './dto/dashboard-filter.dto';
import { Prisma } from '@prisma/client';

@Injectable()
export class OcaOmnixService {
  constructor(private readonly prisma: PrismaService) {}

async getExecutiveSummary(filter: DashboardFilterDto) {
    // 1. DEFINE CTE (Reusable "Virtual Table")
    // We normalize the timestamp to "ticket_timestamp" for easier filtering
    const unifiedCte = `
        WITH "UnifiedTickets" AS (
            SELECT 
                "last_status", 
                "statusTiket", 
                "inSla",
                "ticket_created" as "ticket_timestamp"
            FROM "RawOca"
            -- We don't filter by date inside the CTE anymore because 
            -- the sub-queries need different date ranges.
            
            UNION ALL

            SELECT 
                "ticket_status_name" as "last_status", 
                "statusTiket", 
                "inSla",
                "date_start_interaction" as "ticket_timestamp"
            FROM "RawOmnix"
        )
    `;

    // 2. RUN 3 QUERIES IN PARALLEL
    const [summaryResult, dailyResult, hourlyResult] = await Promise.all([
        
        // A. SUMMARY METRICS (Uses User's Selected Date Range)
        this.prisma.$queryRawUnsafe<any[]>(`
            ${unifiedCte}
            SELECT 
                COUNT(*) FILTER (WHERE "statusTiket" = true)::int as "totalTickets",
                COUNT(*) FILTER (WHERE "last_status" = 'Open' AND "statusTiket" = true)::int as "totalOpen",
                COUNT(*) FILTER (WHERE "last_status" = 'Closed' AND "statusTiket" = true)::int as "totalClosed",
                CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                     THEN ROUND(
                        (COUNT(*) FILTER (WHERE "inSla")::decimal / 
                        COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                     )
                     ELSE 0 
                END as "slaPercentage"
            FROM "UnifiedTickets"
            WHERE "ticket_timestamp" BETWEEN $1::timestamp AND $2::timestamp
        `, filter.startDate, filter.endDate),

        // B. DAILY TREND (EndDate and 6 days before it = 7 days total)
        this.prisma.$queryRawUnsafe<any[]>(`
            ${unifiedCte}
            SELECT 
                TO_CHAR("ticket_timestamp", 'YYYY-MM-DD') as "date",
                COUNT(*)::int as "value",
                CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                     THEN ROUND(
                        (COUNT(*) FILTER (WHERE "inSla")::decimal / 
                        COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                     )
                     ELSE 0 
                END as "sla"
            FROM "UnifiedTickets"
            WHERE "ticket_timestamp" >= ($1::date - INTERVAL '6 days') 
              AND "ticket_timestamp" <  ($1::date + INTERVAL '1 day') -- Include full endDate
            GROUP BY 1
            ORDER BY 1 ASC
        `, filter.endDate),

        // C. 3-HOUR INTERVAL TREND (Only for EndDate)
        // Logic: (Hour / 3) * 3 gives us 0, 3, 6, 9, 12...
        this.prisma.$queryRawUnsafe<any[]>(`
            ${unifiedCte}
            SELECT 
                TRIM(TO_CHAR((EXTRACT(HOUR FROM "ticket_timestamp")::int / 3) * 3, '00')) || ':00' as "time_bucket",
                COUNT(*)::int as "created",
                COUNT(*) FILTER (WHERE "last_status" = 'Closed')::int as "solved"
            FROM "UnifiedTickets"
            WHERE "ticket_timestamp" >= $1::date 
              AND "ticket_timestamp" <  ($1::date + INTERVAL '1 day')
            GROUP BY 1
            ORDER BY 1 ASC
        `, filter.endDate)
    ]);

    // 3. RETURN COMBINED RESPONSE
    return {
        ...summaryResult[0], // Spread summary metrics (totalTickets, etc.)
        dailyTrend: dailyResult,
        hourlyTrend: hourlyResult
    };
}

  // ---------------------------------------------------------
  // 2. CHANNEL BREAKDOWN (The Complex Pivot)
  // ---------------------------------------------------------
  async getChannelStats(filter: DashboardFilterDto) {
    const stats = await this.prisma.$queryRaw<any[]>`
    WITH "UnifiedData" AS (
      -- 1. DATA FROM OCA (Your existing structure)
      SELECT 
        "channel", "statusTiket", "inSla", "last_status", "product", 
        "resolve_time", "ticket_created", "isFcr", "isPareto",
        'OCA' as "source_origin" -- Tagging the source just in case
      FROM "RawOca"
      WHERE "ticket_created" BETWEEN ${filter.startDate}::timestamp AND ${filter.endDate}::timestamp

      UNION ALL

      -- 2. DATA FROM OMNIX (Map your specific columns here)
      SELECT 
        "channel_name" as "channel",                              -- e.g. 'instagram' or 'whatsapp'
        "statusTiket",                          -- MAP: Omnix column -> Standard Name
        "inSla",                                    -- MAP: Omnix column -> Standard Name
        "ticket_status_name" as "last_status",              -- MAP: Omnix column -> Standard Name
        "product",                                 -- MAP: Hardcode if Omnix doesn't have product types
        "date_close" as "resolve_time",             -- MAP: Omnix timestamp -> Standard Name
        "date_start_interaction" as "ticket_created",       -- MAP: Omnix timestamp -> Standard Name
        "isFcr",                                    -- MAP: Omnix column -> Standard Name
        "isPareto",                                    -- MAP: Default to false if Omnix lacks this
        'OMNIX' as "source_origin"
      FROM "RawOmnix"
      WHERE "date_start_interaction" BETWEEN ${filter.startDate}::timestamp AND ${filter.endDate}::timestamp
    )

    -- 3. THE AGGREGATION (Runs on the combined result above)
    SELECT
        channel,
        source_origin, -- Grab this so we know which table to query for details later
        COUNT(*) FILTER (WHERE "statusTiket" = true)::int as "total",
        
        -- % SLA (Logic reused exactly as is!)
        CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
             THEN ROUND((COUNT(*) FILTER (WHERE "inSla")::decimal / NULLIF(COUNT(*) FILTER (WHERE "statusTiket" = true),0)) * 100, 2)
             ELSE 0 END as "pctSla",

        -- Basic Counts
        COUNT(*) FILTER (WHERE "last_status" = 'Open' AND "statusTiket" = true)::int as "open",
        COUNT(*) FILTER (WHERE ("last_status" = 'Closed' OR "last_status" = 'Close')  AND "statusTiket" = true)::int as "closed",

        -- Product Specifics
        COUNT(*) FILTER (WHERE "last_status" = 'Open' AND "product" = 'CONNECTIVITY')::int as "connOpen",
        COUNT(*) FILTER (WHERE "last_status" = 'Open' AND "product" = 'SOLUTION')::int as "solOpen",
        
        -- Resolve Time Logic
        COUNT(*) FILTER (WHERE "product" = 'CONNECTIVITY' AND ("resolve_time" - "ticket_created") > interval '3 hours')::int as "connOver3h",
        COUNT(*) FILTER (WHERE "product" = 'SOLUTION' AND ("resolve_time" - "ticket_created") > interval '6 hours')::int as "solOver6h",

        -- FCR Stats
        COUNT(*) FILTER (WHERE NOT "isFcr")::int as "nonFcrCount",
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "isFcr")::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctFcr",
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE NOT "isFcr")::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctNonFcr",

        -- Pareto Stats
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "isPareto" = true)::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctPareto",
        CASE WHEN COUNT(*) > 0 THEN ROUND((COUNT(*) FILTER (WHERE "isPareto" = false)::decimal / COUNT(*)) * 100, 2) ELSE 0 END as "pctNotPareto"

    FROM "UnifiedData"
    GROUP BY "channel", "source_origin"
  `;

    // Fetch "Top Corporate" and "Top KIP" separately because merging them into the GroupBy above is extremely expensive and complex
    // We will map them in code (application-side join)
    const enhancedStats = await Promise.all(stats.map(async (stat) => {
        const topCorp = await this.getTopEntityForChannel(filter, stat.channel, 'nama_perusahaan', stat.source_origin);
        const topKip = await this.getTopEntityForChannel(filter, stat.channel, 'detail_category', stat.source_origin);
        return { ...stat, topCorporate: topCorp, topKip: topKip };
    }));

    return enhancedStats;
  }

  private async getTopEntityForChannel(
      filter: DashboardFilterDto, 
      channel: string, 
      metricType: 'nama_perusahaan' | 'detail_category', 
      source: 'OCA' | 'OMNIX'
  ) {
      let tableName = '';
      let metricColumn = ''; // The target column (Company or Category)
      let dateColumn = '';
      let channelColumn = ''; // <--- NEW: Dynamic channel column

      // 1. CONFIGURE MAPPING BASED ON SOURCE
      if (source === 'OCA') {
          tableName = '"RawOca"';
          metricColumn = `"${metricType}"`; // e.g. "nama_perusahaan"
          dateColumn = '"ticket_created"';
          channelColumn = '"channel"'; // OCA uses "channel"
      } else {
          tableName = '"RawOmnix"';
          dateColumn = '"date_start_interaction"';
          channelColumn = '"channel_name"'; // <--- OMNIX uses "channel_name"
          
          // Map the metric types to Omnix columns
          if (metricType === 'nama_perusahaan') {
              metricColumn = '"ticket_perusahaan"'; // Replace with actual Omnix column
          } else {
              metricColumn = '"subCategory"'; // Replace with actual Omnix column
          }
      }

      // 2. RUN QUERY
      // We use ${channelColumn} in the WHERE clause instead of hardcoding "channel"
      const result = await this.prisma.$queryRawUnsafe<any[]>(`
        SELECT ${metricColumn} as name, COUNT(*)::int as total, COUNT(*) FILTER (WHERE "eskalasi" <> '')::int as ticket, ROUND((COUNT(*) FILTER (WHERE "isFcr")::decimal / COUNT(*)) * 100, 2) as "pctFcr"
        FROM ${tableName}
        WHERE ${channelColumn} = $1 
          AND ${dateColumn} BETWEEN $2::timestamp AND $3::timestamp
        GROUP BY ${metricColumn}
        ORDER BY total DESC
        LIMIT 5
      `, channel, filter.startDate, filter.endDate);

      return result || [];
  }

  // ---------------------------------------------------------
  // 3. ESCALATION TYPE LIST (Paginated & Searchable)
  // ---------------------------------------------------------
  async getEscalationSummary(query: PaginationDto) {
    const { page, limit, search, startDate, endDate } = query;
    const skip = ((page? page : 1) - 1) * (limit? limit:10);
    const limitVal = limit ? limit : 10;

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
            skip: Number(skip),
            take: Number(limitVal),
            orderBy: { ticketCreated: 'desc' }
        })
    ]);

    return { 
        summary, 
        list: { data, total, page, limitVal } 
    };
  }

  // ---------------------------------------------------------
  // 4. VIP & PARETO STATS
  // ---------------------------------------------------------
    async getSpecialAccountStats(filter: DashboardFilterDto, type: 'VIP' | 'PARETO') {
        const isVip = type === 'VIP';
        
        // 1. Logic: Define the filter condition
        // This applies to the "Unified" dataset created below
        const condition = isVip 
            ? `WHERE "isVip" = true` 
            : `WHERE "isPareto" = true AND "isVip" = false`;

        // 2. The CTE (Common Table Expression)
        // We define this once to normalize columns from Omnix to match OCA
        const unifiedCte = `
            WITH "UnifiedSpecial" AS (
                -- OCA DATA
                SELECT 
                    "isVip", 
                    "isPareto", 
                    "last_status", 
                    "resolve_time", 
                    "ticket_created", 
                    "nama_perusahaan" 
                FROM "RawOca"
                WHERE "ticket_created" BETWEEN $1::timestamp AND $2::timestamp

                UNION ALL

                -- OMNIX DATA (Mapped)
                SELECT 
                    "isVip",                   -- Map Omnix VIP column
                    "isPareto",             -- Map Omnix Pareto (or false if not exists)
                    "ticket_status_name" as "last_status",             -- Map Status
                    "date_close" as "resolve_time",       -- Map Resolve Time
                    "date_start_interaction" as "ticket_created",      -- Map Created Time
                    "ticket_perusahaan" as "nama_perusahaan"   -- Map Company Name
                FROM "RawOmnix"
                WHERE "date_start_interaction" BETWEEN $1::timestamp AND $2::timestamp
            )
        `;

        // 3. Query 1: General Stats (Open tickets & Over 3h)
        // Note: We inject the CTE at the top
        const rawQuery = `
            ${unifiedCte}
            SELECT 
                COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "openTickets",
                COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h"
            FROM "UnifiedSpecial"
            ${condition} 
            -- Note: Date filter is already applied inside the CTE for performance
        `;

        // 4. Query 2: Top 10 Corp List
        const corpQuery = `
            ${unifiedCte}
            SELECT "nama_perusahaan", COUNT(*)::int as total
            FROM "UnifiedSpecial"
            ${condition}
            GROUP BY "nama_perusahaan"
            ORDER BY total DESC
            LIMIT 10
        `;

        // Execute both in parallel
        const [stats, topCorps] = await Promise.all([
            this.prisma.$queryRawUnsafe<any[]>(rawQuery, filter.startDate, filter.endDate),
            this.prisma.$queryRawUnsafe<any[]>(corpQuery, filter.startDate, filter.endDate)
        ]);

        return { stats: stats[0] || { openTickets: 0, over3h: 0 }, topCorps };
    }

 async getTopKipPerCompany(query: PaginationDto) {
    const { page, limit, search, startDate, endDate } = query;
    const offset = ((page ? page : 1) - 1) * (limit ? limit : 10);
    const limitVal = limit ? limit : 10;

    // 1. DEFINE CTE (Reusable "Virtual Table")
    const unifiedCte = `
        WITH "UnifiedData" AS (
            SELECT 
                "nama_perusahaan", 
                "detail_category",
                "statusTiket",
                "inSla"
            FROM "RawOca"
            WHERE "ticket_created" BETWEEN $1::timestamp AND $2::timestamp
            
            UNION ALL
            
            SELECT 
                "ticket_perusahaan" as "nama_perusahaan", 
                "subCategory" as "detail_category",
                "statusTiket",
                "inSla"
            FROM "RawOmnix"
            WHERE "date_start_interaction" BETWEEN $1::timestamp AND $2::timestamp
        )
    `;

    // ---------------------------------------------------------
    // STEP 1: Get Paginated List of Companies (With SEARCH)
    // ---------------------------------------------------------
    // We dynamically add the search condition here
    const searchCondition = search ? `AND "nama_perusahaan" ILIKE '%' || $3 || '%'` : '';

    const companyQuery = `
        ${unifiedCte}
        SELECT 
            "nama_perusahaan", 
            COUNT(*)::int as total_tickets,
            -- Company Level SLA
            CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "inSla")::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "company_sla"
        FROM "UnifiedData"
        WHERE 1=1
        ${searchCondition}  -- <--- Search is applied here
        GROUP BY "nama_perusahaan"
        ORDER BY total_tickets DESC
        LIMIT ${limitVal} OFFSET ${offset}
    `;

    // Define params for Step 1
    // If search exists, we pass 3 params ($1, $2, $3). If not, just 2 ($1, $2).
    const companyParams = search ? [startDate, endDate, search] : [startDate, endDate];

    const companies = await this.prisma.$queryRawUnsafe<any[]>(companyQuery, ...companyParams);

    if (companies.length === 0) return []; // Stop if no companies match the search

    // ---------------------------------------------------------
    // STEP 2: Get Top 3 KIPs for the FOUND Companies
    // ---------------------------------------------------------
    const companyNames = companies.map(c => c.nama_perusahaan);
    
    // IMPORTANT: We create a fresh parameter array for Step 2.
    // We ignore the 'search' param here because we already have the specific list of companies.
    // Params will be: [$1=Start, $2=End, $3=Comp1, $4=Comp2, ...]
    const kipsParams = [startDate, endDate, ...companyNames];

    // We map placeholders starting from index 3 ($3)
    const placeholders = companyNames.map((_, i) => `$${i + 3}`).join(', ');

    const kipsQuery = `
        ${unifiedCte}
        , RankedKip AS (
            SELECT 
                "nama_perusahaan", 
                "detail_category", 
                COUNT(*)::int as kip_count,
                -- KIP (Category) Level SLA
                CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                     THEN ROUND(
                        (COUNT(*) FILTER (WHERE "inSla")::decimal / 
                         COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                     )
                     ELSE 0 
                END as "kip_sla",
                ROW_NUMBER() OVER(PARTITION BY "nama_perusahaan" ORDER BY COUNT(*) DESC)::int as rn
            FROM "UnifiedData"
            WHERE "nama_perusahaan" IN (${placeholders}) 
            GROUP BY "nama_perusahaan", "detail_category"
        )
        SELECT * FROM RankedKip WHERE rn <= 3
    `;

    const kips = await this.prisma.$queryRawUnsafe<any[]>(kipsQuery, ...kipsParams);

    // ---------------------------------------------------------
    // STEP 3: Map Results
    // ---------------------------------------------------------
    return companies.map(comp => {
        return {
            company: comp.nama_perusahaan,
            totalTickets: comp.total_tickets,
            companySla: comp.company_sla,
            topKips: kips
                .filter(k => k.nama_perusahaan === comp.nama_perusahaan)
                .map(k => ({
                    detail_category: k.detail_category,
                    kip_count: k.kip_count,
                    kip_sla: k.kip_sla,
                    rn: k.rn
                }))
        };
    });
}
  
  // ---------------------------------------------------------
  // 6. PRODUCT BREAKDOWN (Connectivity, Solution, etc)
  // ---------------------------------------------------------
  async getProductBreakdown(filter: DashboardFilterDto) {
    // 1. DEFINE CTE (Added 'general_category' normalization)
    const unifiedCte = `
        WITH "UnifiedData" AS (
            -- Table 1: RawOca
            SELECT 
                "product", 
                "last_status", 
                "resolve_time", 
                "ticket_created", 
                "statusTiket", 
                "inSla", 
                "detail_category",               -- Used for Top KIPs (existing)
                "sub_category" as "general_category" -- NEW: Used for Top 5 Category
            FROM "RawOca"
            WHERE "ticket_created" BETWEEN $1::timestamp AND $2::timestamp
            
            UNION ALL
            
            -- Table 2: RawOmnix
            SELECT 
                "product", 
                "ticket_status_name" as "last_status", 
                "date_close" as "resolve_time", 
                "date_start_interaction" as "ticket_created", 
                "statusTiket", 
                "inSla", 
                "subCategory" as "detail_category", -- Used for Top KIPs (existing)
                "category" as "general_category"    -- NEW: Used for Top 5 Category
            FROM "RawOmnix"
            WHERE "date_start_interaction" BETWEEN $1::timestamp AND $2::timestamp
        )
    `;

    // 2. FETCH ALL DAILY TRENDS IN ONE GO (Optimization)
    // We fetch daily stats for ALL products here to avoid N+1 queries in the loop
    const dailyStatsRaw = await this.prisma.$queryRawUnsafe<any[]>(`
        ${unifiedCte}
        SELECT 
            "product",
            TO_CHAR("ticket_created", 'YYYY-MM-DD') as "date",
            COUNT(*)::int as "total",
            CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "inSla")::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "dailySla"
        FROM "UnifiedData"
        WHERE "product" IN ('CONNECTIVITY', 'SOLUTION', 'DADS')
        GROUP BY "product", TO_CHAR("ticket_created", 'YYYY-MM-DD')
        ORDER BY "date" ASC
    `, filter.startDate, filter.endDate);

    // 3. MAIN METRIC AGGREGATION (Product Level)
    const products = await this.prisma.$queryRawUnsafe<any[]>(`
        ${unifiedCte}
        SELECT 
            "product",
            COUNT(*)::int as "total",
            COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "open",
            COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h",
            CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "inSla")::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "pctSla"
        FROM "UnifiedData"
        WHERE "product" IN ('CONNECTIVITY', 'SOLUTION', 'DADS')
        GROUP BY "product"
    `, filter.startDate, filter.endDate);

    // 4. ATTACH DETAILS (Top KIPs & Top Categories)
    const detailed = await Promise.all(products.map(async (p) => {
        
        // A. Existing: Top 10 KIPs (detail_category)
        const topKips = await this.prisma.$queryRawUnsafe<any[]>(`
            ${unifiedCte}
            SELECT 
                "detail_category", 
                COUNT(*)::int as total,
                CASE WHEN COUNT(*) > 0 
                     THEN ROUND((COUNT(*) FILTER (WHERE "inSla")::decimal / COUNT(*)) * 100, 2) 
                     ELSE 0 
                END as "kipSla"
            FROM "UnifiedData"
            WHERE "product" = $3
            GROUP BY "detail_category"
            ORDER BY total DESC
            LIMIT 10
        `, filter.startDate, filter.endDate, p.product);

        // B. New Requirement: Top 5 General Categories (general_category)
        const topCategories = await this.prisma.$queryRawUnsafe<any[]>(`
            ${unifiedCte}
            SELECT 
                "general_category", 
                COUNT(*)::int as total,
                CASE WHEN COUNT(*) > 0 
                     THEN ROUND((COUNT(*) FILTER (WHERE "inSla")::decimal / COUNT(*)) * 100, 2) 
                     ELSE 0 
                END as "catSla"
            FROM "UnifiedData"
            WHERE "product" = $3
            GROUP BY "general_category"
            ORDER BY total DESC
            LIMIT 5
        `, filter.startDate, filter.endDate, p.product);

        // C. New Requirement: Attach Daily Trends (mapped from step 2)
        const trend = dailyStatsRaw.filter((d) => d.product === p.product);

        return { 
            ...p, 
            topKips, 
            topCategories, 
            trend 
        };
    }));

    return detailed;
}
}