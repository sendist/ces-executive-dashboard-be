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
                "ticket_created" as "ticket_timestamp",
                "channel"
            FROM "RawOca"
            -- We don't filter by date inside the CTE anymore because 
            -- the sub-queries need different date ranges.
            
            UNION ALL

            SELECT 
                "ticket_status_name" as "last_status", 
                "statusTiket", 
                "inSla",
                "date_start_interaction" as "ticket_timestamp",
                "channel_name" as "channel"
            FROM "RawOmnix"
        )
    `;

    // 2. RUN 3 QUERIES IN PARALLEL
    const [summaryResult, dailyResult, hourlyResult] = await Promise.all([
      // A. SUMMARY METRICS (Uses User's Selected Date Range)
      this.prisma.$queryRawUnsafe<any[]>(
        `
            ${unifiedCte}
        SELECT 
            COUNT(*)::int AS "totalCreated",

            -- totalTickets: filtered by channel
            COUNT(*) FILTER (
                WHERE "statusTiket"
                AND "channel" ILIKE ANY (ARRAY['email', 'livechat', 'whatsapp', 'ig message'])
            )::int AS "totalTickets",

            -- totalOpen: filtered by channel + status
            COUNT(*) FILTER (
                WHERE "statusTiket"
                AND "channel" ILIKE ANY (ARRAY['email', 'livechat', 'whatsapp', 'ig message'])
                AND NOT ("last_status" ILIKE ANY (ARRAY['closed', 'resolved']))
            )::int AS "totalOpen",

            -- totalClosed: filtered by channel + status
            COUNT(*) FILTER (
                WHERE "statusTiket"
                AND "channel" ILIKE ANY (ARRAY['email', 'livechat', 'whatsapp', 'ig message'])
                AND "last_status" ILIKE ANY (ARRAY['closed', 'resolved'])
            )::int AS "totalClosed",

            -- SLA percentage: filtered by channel
            CASE
                WHEN COUNT(*) FILTER (
                    WHERE "statusTiket"
                    AND "channel" ILIKE ANY (ARRAY['email', 'livechat', 'whatsapp', 'ig message'])
                ) > 0 THEN
                    ROUND(
                        COUNT(*) FILTER (
                            WHERE "inSla"  AND "statusTiket"
                            AND "statusTiket"
                            AND "channel" ILIKE ANY (ARRAY['email', 'livechat', 'whatsapp', 'ig message'])
                        )::numeric
                        / COUNT(*) FILTER (
                            WHERE "statusTiket"
                            AND "channel" ILIKE ANY (ARRAY['email', 'livechat', 'whatsapp', 'ig message'])
                        )::numeric
                        * 100,
                        2
                    )
                ELSE 0
            END AS "slaPercentage"

        FROM "UnifiedTickets"
        WHERE "ticket_timestamp" >= $1::timestamptz
        AND "ticket_timestamp" <  $2::timestamptz;
        `,
        filter.startDate,
        filter.endDate,
      ),

      // B. DAILY TREND (EndDate and 6 days before it = 7 days total)
      this.prisma.$queryRawUnsafe<any[]>(
        `
            ${unifiedCte}
        SELECT 
            TO_CHAR("ticket_timestamp" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Jakarta', 'YYYY-MM-DD') AS "date",
            COUNT(*)::int AS "value",
            CASE 
                WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 THEN
                    ROUND(
                        COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal
                        / COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal
                        * 100,
                        2
                    )
                ELSE 0
            END AS "sla"
        FROM "UnifiedTickets"
        WHERE "ticket_timestamp" >= ($1::date - INTERVAL '6 days') AT TIME ZONE 'Asia/Jakarta' AT TIME ZONE 'UTC'
        AND "ticket_timestamp" <  ($1::date + INTERVAL '1 day') AT TIME ZONE 'Asia/Jakarta' AT TIME ZONE 'UTC'
        GROUP BY 1
        ORDER BY 1 ASC;

        `,
        filter.endDate,
      ),

      // C. 3-HOUR INTERVAL TREND (Only for EndDate)
      // Logic: (Hour / 3) * 3 gives us 0, 3, 6, 9, 12...
      this.prisma.$queryRawUnsafe<any[]>(
        `
            ${unifiedCte}
SELECT 
    TRIM(
        TO_CHAR(
            (EXTRACT(HOUR FROM (ticket_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Jakarta'))::int / 3) * 3,
            '00'
        )
    ) || ':00' AS time_bucket,
    
    COUNT(*)::int AS created,
    
    COUNT(*) FILTER (WHERE last_status ILIKE ANY (ARRAY['closed','resolved']))::int AS solved

FROM "UnifiedTickets"
WHERE ticket_timestamp >= ($1::date - INTERVAL '7 hours')  -- 00:00 WIB → UTC
  AND ticket_timestamp <  (($1::date + INTERVAL '1 day') - INTERVAL '7 hours') -- 00:00 next day WIB → UTC
GROUP BY 1
ORDER BY 1 ASC;
        `,
        filter.endDate,
      ),
    ]);

    // 3. RETURN COMBINED RESPONSE
    return {
      ...summaryResult[0], // Spread summary metrics (totalTickets, etc.)
      dailyTrend: dailyResult,
      hourlyTrend: hourlyResult,
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
             THEN ROUND((COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / NULLIF(COUNT(*) FILTER (WHERE "statusTiket" = true),0)) * 100, 2)
             ELSE 0 END as "pctSla",

        -- Basic Counts
        COUNT(*) FILTER (WHERE NOT ("last_status" ILIKE 'Closed' OR "last_status" ILIKE 'resolved') AND "statusTiket" = true)::int as "open",
        COUNT(*) FILTER (WHERE ("last_status" ILIKE 'Closed' OR "last_status" ILIKE 'resolved')  AND "statusTiket" = true)::int as "closed",

        -- Product Specifics
        COUNT(*) FILTER (WHERE NOT ("last_status" ILIKE 'Closed' OR "last_status" ILIKE 'resolved') AND "product" = 'CONNECTIVITY')::int as "connOpen",
        COUNT(*) FILTER (WHERE NOT ("last_status" ILIKE 'Closed' OR "last_status" ILIKE 'resolved') AND "product" = 'SOLUTION')::int as "solOpen",
        
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
    const enhancedStats = await Promise.all(
      stats.map(async (stat) => {
        const topCorp = await this.getTopEntityForChannel(
          filter,
          stat.channel,
          'nama_perusahaan',
          stat.source_origin,
        );
        const topKip = await this.getTopEntityForChannel(
          filter,
          stat.channel,
          'detail_category',
          stat.source_origin,
        );
        return { ...stat, topCorporate: topCorp, topKip: topKip };
      }),
    );

    return enhancedStats;
  }

  private async getTopEntityForChannel(
    filter: DashboardFilterDto,
    channel: string,
    metricType: 'nama_perusahaan' | 'detail_category',
    source: 'OCA' | 'OMNIX',
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
    const result = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT ${metricColumn} as name, COUNT(*)::int as total, COUNT(*) FILTER (WHERE "eskalasi" <> '')::int as ticket, ROUND((COUNT(*) FILTER (WHERE "isFcr")::decimal / COUNT(*)) * 100, 2) as "pctFcr"
        FROM ${tableName}
        WHERE ${channelColumn} = $1 
          AND (TRIM(${metricColumn}) <> '-' AND TRIM(${metricColumn}) <> '' AND ${metricColumn} NOTNULL)
          AND "statusTiket"
          AND ${dateColumn} BETWEEN $2::timestamp AND $3::timestamp
        GROUP BY ${metricColumn}
        ORDER BY total DESC
        LIMIT 5
      `,
      channel,
      filter.startDate,
      filter.endDate,
    );

    return result || [];
  }

  // ---------------------------------------------------------
  // 3. ESCALATION TYPE LIST (Paginated & Searchable)
  // ---------------------------------------------------------
  async getEscalationSummary(query: PaginationDto) {
    const { page, limit, search, startDate, endDate } = query;
    const skip = ((page ? page : 1) - 1) * (limit ? limit : 10);
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
      ticketCreated: {
        gte: new Date(startDate ? startDate : new Date()),
        lte: new Date(endDate ? endDate : new Date()),
      },
      OR: search
        ? [
            { ticketNumber: { contains: search, mode: 'insensitive' } },
            { idRemedyNo: { contains: search, mode: 'insensitive' } },
          ]
        : undefined,
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
          eskalasi: true,
        },
        skip: Number(skip),
        take: Number(limitVal),
        orderBy: { ticketCreated: 'desc' },
      }),
    ]);

    return {
      summary,
      list: { data, total, page, limitVal },
    };
  }

  // ---------------------------------------------------------
  // 4. VIP & PARETO STATS
  // ---------------------------------------------------------
  async getSpecialAccountStats(
    filter: DashboardFilterDto,
    type: 'VIP' | 'PARETO',
  ) {
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
                    "nama_perusahaan",
                    "detail_category",
                    "inSla",
                    "statusTiket"
                FROM "RawOca"
                WHERE "ticket_created" BETWEEN $1::timestamp AND $2::timestamp AND "statusTiket"
                    AND (TRIM("nama_perusahaan") <> '-' AND TRIM("nama_perusahaan") <> '' AND "nama_perusahaan" NOTNULL)

                UNION ALL

                -- OMNIX DATA (Mapped)
                SELECT 
                    "isVip",                   -- Map Omnix VIP column
                    "isPareto",             -- Map Omnix Pareto (or false if not exists)
                    "ticket_status_name" as "last_status",             -- Map Status
                    "date_close" as "resolve_time",       -- Map Resolve Time
                    "date_start_interaction" as "ticket_created",      -- Map Created Time
                    "ticket_perusahaan" as "nama_perusahaan",   -- Map Company Name
                    "subCategory" as "detail_category",
                    "inSla",
                    "statusTiket"
                FROM "RawOmnix"
                WHERE "date_start_interaction" BETWEEN $1::timestamp AND $2::timestamp AND "statusTiket"
                  AND (TRIM("ticket_perusahaan") <> '-' AND TRIM("ticket_perusahaan") <> '' AND "ticket_perusahaan" NOTNULL)

            )
        `;

    // 3. Query 1: General Stats (Open tickets & Over 3h)
    // Note: We inject the CTE at the top
    const rawQuery = `
            ${unifiedCte}
            SELECT 
                    COUNT(*) FILTER (
        WHERE "statusTiket"
          AND NOT ("last_status" ILIKE ANY (ARRAY['closed','resolved']))
    )::int AS "openTickets",
                COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h"
            FROM "UnifiedSpecial"
            ${condition} 
            -- Note: Date filter is already applied inside the CTE for performance
        `;

    // 4. Query 2: Top 10 Corp List
    const corpQuery = `
            ${unifiedCte}
            SELECT TRIM("nama_perusahaan") as nama_perusahaan, COUNT(*)::int as total
            FROM "UnifiedSpecial"
            ${condition}
            GROUP BY TRIM("nama_perusahaan") 
            ORDER BY total DESC
            LIMIT 10
        `;

    // 5. Query 3: Top 10 KIP List (New Requirement)
    const kipQuery = `
            ${unifiedCte}
            SELECT 
                "detail_category",
                COUNT(*) FILTER (WHERE "inSla" = true AND "statusTiket")::int as "inSla",
                COUNT(*) FILTER (WHERE "inSla" = false AND "statusTiket")::int as "outSla",
                COUNT(*)::int as "total"
            FROM "UnifiedSpecial"
            ${condition}
            GROUP BY "detail_category"
            ORDER BY "total" DESC
            LIMIT 10
    `;

    // Execute both in parallel
    const [stats, topCorps, topKips] = await Promise.all([
      this.prisma.$queryRawUnsafe<any[]>(
        rawQuery,
        filter.startDate,
        filter.endDate,
      ),
      this.prisma.$queryRawUnsafe<any[]>(
        corpQuery,
        filter.startDate,
        filter.endDate,
      ),
      this.prisma.$queryRawUnsafe<any[]>(
        kipQuery,
        filter.startDate,
        filter.endDate,
      ),
    ]);

    return { stats: stats[0] || { openTickets: 0, over3h: 0 }, topCorps, topKips };
  }

 async getTopKipPerCompany(query: PaginationDto) {
    const { page, limit, search, startDate, endDate } = query;
    // Default values if not provided
    const pageVal = page ? page : 1;
    const limitVal = limit ? limit : 10;
    const offset = (pageVal - 1) * limitVal;

    // 1. DEFINE CTE (Reusable "Virtual Table")
    // (This remains exactly the same)
    const unifiedCte = `
        WITH "UnifiedData" AS (
            SELECT 
                "nama_perusahaan", 
                "detail_category",
                "statusTiket",
                "inSla"
            FROM "RawOca"
            WHERE "ticket_created" BETWEEN $1::timestamp AND $2::timestamp AND "statusTiket"
              AND (TRIM("nama_perusahaan") <> '-' AND TRIM("nama_perusahaan") <> '' AND "nama_perusahaan" NOTNULL)
            
            UNION ALL
            
            SELECT 
                "ticket_perusahaan" as "nama_perusahaan", 
                "subCategory" as "detail_category",
                "statusTiket",
                "inSla"
            FROM "RawOmnix"
            WHERE "date_start_interaction" BETWEEN $1::timestamp AND $2::timestamp AND "statusTiket"
            AND (TRIM("ticket_perusahaan") <> '-' AND TRIM("ticket_perusahaan") <> '' AND "ticket_perusahaan" NOTNULL)
        )
    `;

    // ---------------------------------------------------------
    // PREPARE PARAMS
    // ---------------------------------------------------------
    const searchCondition = search
      ? `AND "nama_perusahaan" ILIKE '%' || $3 || '%'`
      : '';

    // Parameters for both Count and Main Query
    const queryParams = search
      ? [startDate, endDate, search]
      : [startDate, endDate];

    // ---------------------------------------------------------
    // STEP 0: Get TOTAL Count (For Pagination)
    // ---------------------------------------------------------
    // We count distinct companies matching the filter, ignoring LIMIT/OFFSET
    const countQuery = `
        ${unifiedCte}
        SELECT COUNT(DISTINCT "nama_perusahaan")::int as total
        FROM "UnifiedData"
        WHERE 1=1
        ${searchCondition}
    `;

    // ---------------------------------------------------------
    // STEP 1: Get Paginated List of Companies
    // ---------------------------------------------------------
    const companyQuery = `
        ${unifiedCte}
        SELECT 
            "nama_perusahaan", 
            COUNT(*)::int as total_tickets,
            CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "company_sla"
        FROM "UnifiedData"
        WHERE 1=1
        ${searchCondition}
        GROUP BY "nama_perusahaan"
        ORDER BY total_tickets DESC
        LIMIT ${limitVal} OFFSET ${offset}
    `;

    // Run Count and Data queries in parallel for performance
    const [totalResult, companies] = await Promise.all([
        this.prisma.$queryRawUnsafe<any[]>(countQuery, ...queryParams),
        this.prisma.$queryRawUnsafe<any[]>(companyQuery, ...queryParams)
    ]);

    const totalRows = totalResult[0]?.total || 0;

    // Handle empty case
    if (companies.length === 0) {
        return {
            data: [],
            meta: {
                page: Number(pageVal),
                limit: Number(limitVal),
                total: Number(totalRows),
                totalPages: 0
            }
        };
    }

    // ---------------------------------------------------------
    // STEP 2: Get Top 3 KIPs (Logic Unchanged)
    // ---------------------------------------------------------
    const companyNames = companies.map((c) => c.nama_perusahaan);
    const kipsParams = [startDate, endDate, ...companyNames];
    const placeholders = companyNames.map((_, i) => `$${i + 3}`).join(', ');

    const kipsQuery = `
        ${unifiedCte}
        , RankedKip AS (
            SELECT 
                "nama_perusahaan", 
                "detail_category", 
                COUNT(*)::int as kip_count,
                CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                     THEN ROUND(
                        (COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / 
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

    const kips = await this.prisma.$queryRawUnsafe<any[]>(
        kipsQuery,
        ...kipsParams,
    );

    // ---------------------------------------------------------
    // STEP 3: Map Results & Return with Meta
    // ---------------------------------------------------------
    const mappedData = companies.map((comp) => {
        return {
            company: comp.nama_perusahaan,
            totalTickets: comp.total_tickets,
            companySla: comp.company_sla,
            topKips: kips
                .filter((k) => k.nama_perusahaan === comp.nama_perusahaan)
                .map((k) => ({
                    detail_category: k.detail_category,
                    kip_count: k.kip_count,
                    kip_sla: k.kip_sla,
                    rn: k.rn,
                })),
        };
    });

    return {
        data: mappedData,
        meta: {
            page: Number(pageVal),
            limit: Number(limitVal),
            total: Number(totalRows),
            totalPages: Math.ceil(Number(totalRows) / Number(limitVal))
        }
    };
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
            WHERE "ticket_created" >= $1::timestamptz AND "ticket_created" < $2::timestamptz AND "statusTiket"
            
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
            WHERE "date_start_interaction" >= $1::timestamp AND "date_start_interaction" < $2::timestamp AND "statusTiket"
        )
    `;

    // 2. FETCH ALL DAILY TRENDS IN ONE GO (Optimization)
    // We fetch daily stats for ALL products here to avoid N+1 queries in the loop
    const dailyStatsRaw = await this.prisma.$queryRawUnsafe<any[]>(
      `
        ${unifiedCte}
        SELECT 
            "product",
            TO_CHAR("ticket_created" + interval '7 hours', 'YYYY-MM-DD') as "date",
            COUNT(*)::int as "total",
            CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "dailySla"
        FROM "UnifiedData"
        WHERE "product" IN ('CONNECTIVITY', 'SOLUTION', 'DADS')
        GROUP BY "product", TO_CHAR("ticket_created" + interval '7 hours', 'YYYY-MM-DD')
        ORDER BY "date" ASC
    `,
      filter.startDate,
      filter.endDate,
    );

    // 3. MAIN METRIC AGGREGATION (Product Level)
    const products = await this.prisma.$queryRawUnsafe<any[]>(
      `
        ${unifiedCte}
        SELECT 
            "product",
            COUNT(*)::int as "total",
            COUNT(*) FILTER (WHERE "last_status" = 'Open')::int as "open",
            COUNT(*) FILTER (WHERE ("resolve_time" - "ticket_created") > interval '3 hours')::int as "over3h",
            CASE WHEN COUNT(*) FILTER (WHERE "statusTiket" = true) > 0 
                 THEN ROUND(
                    (COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / 
                     COUNT(*) FILTER (WHERE "statusTiket" = true)::decimal) * 100, 2
                 )
                 ELSE 0 
            END as "pctSla"
        FROM "UnifiedData"
        WHERE "product" IN ('CONNECTIVITY', 'SOLUTION', 'DADS')
        GROUP BY "product"
    `,
      filter.startDate,
      filter.endDate,
    );

    // 4. ATTACH DETAILS (Top KIPs & Top Categories)
    const detailed = await Promise.all(
      products.map(async (p) => {
        // A. Existing: Top 10 KIPs (detail_category)
        const topKips = await this.prisma.$queryRawUnsafe<any[]>(
          `
            ${unifiedCte}
            SELECT 
                "detail_category", 
                COUNT(*)::int as total,
                CASE WHEN COUNT(*) > 0 
                     THEN ROUND((COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / COUNT(*)) * 100, 2) 
                     ELSE 0 
                END as "kipSla"
            FROM "UnifiedData"
            WHERE "product" = $3 
              AND (TRIM("detail_category") <> '-' AND TRIM("detail_category") <> '' AND "detail_category" NOTNULL)
            GROUP BY "detail_category"
            ORDER BY total DESC
            LIMIT 10
        `,
          filter.startDate,
          filter.endDate,
          p.product,
        );

        // B. New Requirement: Top 5 General Categories (general_category)
        const topCategories = await this.prisma.$queryRawUnsafe<any[]>(
          `
            ${unifiedCte}
            SELECT 
                "general_category", 
                COUNT(*)::int as total,
                CASE WHEN COUNT(*) > 0 
                     THEN ROUND((COUNT(*) FILTER (WHERE "inSla" AND "statusTiket")::decimal / COUNT(*)) * 100, 2) 
                     ELSE 0 
                END as "catSla"
            FROM "UnifiedData"
            WHERE "product" = $3
              AND (TRIM("general_category") <> '-' AND TRIM("general_category") <> '' AND "general_category" NOTNULL)
            GROUP BY "general_category"
            ORDER BY total DESC
            LIMIT 5
        `,
          filter.startDate,
          filter.endDate,
          p.product,
        );

        // C. New Requirement: Attach Daily Trends (mapped from step 2)
        const trend = dailyStatsRaw.filter((d) => d.product === p.product);

        return {
          ...p,
          topKips,
          topCategories,
          trend,
        };
      }),
    );

    return detailed;
  }
}
