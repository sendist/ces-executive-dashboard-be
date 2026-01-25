import { Injectable } from '@nestjs/common';
import { Job } from 'bullmq';
import { PrismaService } from 'prisma/prisma.service';
import * as ExcelJS from 'exceljs';
import { ExcelUtils } from '../excel-utils.helper';
import * as fs from 'fs';

@Injectable()
export class CsatUploadService {
  constructor(private readonly prisma: PrismaService) {}

  async process(job: Job<any, any, string>): Promise<any> {
    const filePath = job.data.path;
    if (!fs.existsSync(filePath)) {
      console.error(`File missing at path: ${filePath}`);
      // Throwing an error here marks the job as FAILED in BullMQ,
      // but it won't crash your entire Node.js server.
      throw new Error(`File not found: ${filePath} - likely a stale job.`);
    }

    const batchSize = 1000;
    let rowsToInsert: any[] = [];
    const affectedDates = new Set<string>();

    // 1. Stream the Excel file
    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});

    for await (const worksheet of workbook) {
      for await (const row of worksheet) {
        if (row.number === 1) continue; // Skip Header

        // SAFE PARSING LOGIC
        // Handle empty dates or invalid scores gracefully
        const createdAtRaw = row.getCell(2).value;
        const answeredAtRaw = row.getCell(4).value;
        const scoreRaw = row.getCell(9).value; // "Numeric" column
        
        if (createdAtRaw) {
          const parsedDate = ExcelUtils.parseExcelDate(createdAtRaw);

          // Check if the parsing actually succeeded
          if (parsedDate) {
            const dateString = parsedDate.toISOString().split('T')[0];
            affectedDates.add(dateString);
          }
        }

        const rowData = {
          // id: this.extractFirstId(row.getCell(7).text),
          createdAt: ExcelUtils.parseExcelDate(createdAtRaw),
          status: row.getCell(3).text,
          // Only parse answeredAt if it exists
          answeredAt: answeredAtRaw
            ? ExcelUtils.parseExcelDate(answeredAtRaw)
            : null,
          customer: row.getCell(5).text,
          ticketNumbers: row.getCell(6).text,
          interactionId: row.getCell(7).text,
          question1: row.getCell(8).text,
          // Parse score to Int, handle nulls
          numeric: scoreRaw ? parseInt(scoreRaw.toString()) : null,
          question2: row.getCell(10).text,
          question3: row.getCell(11).text,
          question4: row.getCell(12).text,
          question5: row.getCell(13).text,
          question6: row.getCell(14).text,
          channel: row.getCell(15).text,
          assignedAgent: row.getCell(16).text,
        };

        rowsToInsert.push(rowData);

        if (rowsToInsert.length >= batchSize) {
          await this.saveBatch(rowsToInsert);
          rowsToInsert = [];
        }
      }
    }

    if (rowsToInsert.length > 0) {
      await this.saveBatch(rowsToInsert);
    }

    const uniqueDates = Array.from(affectedDates).map((d) => new Date(d));

    // 2. RUN SUMMARIZATION
    await this.refreshDailyStats(uniqueDates);

    return { status: 'Completed' };
  }

  // private async saveBatch(rows) {
  //   // skipDuplicates ensures if you upload the same file twice,
  //   // it won't crash on the unique InteractionID constraint.
  //   await this.prisma.rawCsat.createMany({
  //     data: rows,
  //     skipDuplicates: true
  //   });
  // }

  private async saveBatch(rows: any[]) {
    if (rows.length === 0) return;

    // 1. DEDUPLICATE IN MEMORY
    // We use a Map to ensure only one entry per (createdAt + customer) exists in this batch.
    // If duplicates exist in the batch, the LAST one in the list "wins" (overwrites previous).
    const uniqueRowsMap = new Map<string, any>();
    const internalDuplicates: any[] = [];

    for (const row of rows) {
      const uniqueKey = `${row.createdAt.toISOString()}_${row.customer}`;

      if (uniqueRowsMap.has(uniqueKey)) {
        // This is a duplicate within the Excel file itself
        internalDuplicates.push({
          customer: row.customer,
          date: row.createdAt,
          reason: 'Duplicate found inside the same Excel batch',
        });
      } else {
        uniqueRowsMap.set(uniqueKey, row);
      }
    }

    // Log internal duplicates if any
    if (internalDuplicates.length > 0) {
      console.log('internal Duplicates Skipped:', internalDuplicates);
    }

    // Convert back to array
    const cleanRows = Array.from(uniqueRowsMap.values());

    // 2. Map rows to SQL tuple strings
    const values = cleanRows
      .map((row) => {
        return `(
          ${ExcelUtils.formatSqlValue(row.createdAt)},
          ${ExcelUtils.formatSqlValue(row.customer)},
          ${ExcelUtils.formatSqlValue(row.status)},
          ${ExcelUtils.formatSqlValue(row.answeredAt)},
          ${ExcelUtils.formatSqlValue(row.ticketNumbers)},
          ${ExcelUtils.formatSqlValue(row.interactionId)},
          ${ExcelUtils.formatSqlValue(row.question1)},
          ${ExcelUtils.formatSqlValue(row.numeric)},
          ${ExcelUtils.formatSqlValue(row.question2)},
          ${ExcelUtils.formatSqlValue(row.question3)},
          ${ExcelUtils.formatSqlValue(row.question4)},
          ${ExcelUtils.formatSqlValue(row.question5)},
          ${ExcelUtils.formatSqlValue(row.question6)},
          ${ExcelUtils.formatSqlValue(row.channel)},
          ${ExcelUtils.formatSqlValue(row.assignedAgent)}
        )`;
      })
      .join(',');

    // 3. Construct the full Query
    const query = `
      INSERT INTO "RawCsat" (
        "createdAt", "customer", "status", "answeredAt", 
        "ticketNumbers", "interactionId", "question1", "numeric", 
        "question2", "question3", "question4", "question5", 
        "question6", "channel", "assignedAgent"
      )
      VALUES ${values}
      ON CONFLICT ("createdAt", "customer") 
      DO UPDATE SET 
        "status"        = EXCLUDED."status",
        "answeredAt"    = EXCLUDED."answeredAt",
        "ticketNumbers" = EXCLUDED."ticketNumbers",
        "interactionId" = EXCLUDED."interactionId",
        "question1"     = EXCLUDED."question1",
        "numeric"       = EXCLUDED."numeric",
        "question2"     = EXCLUDED."question2",
        "question3"     = EXCLUDED."question3",
        "question4"     = EXCLUDED."question4",
        "question5"     = EXCLUDED."question5",
        "question6"     = EXCLUDED."question6",
        "channel"       = EXCLUDED."channel",
        "assignedAgent" = EXCLUDED."assignedAgent";
    `;

    await this.prisma.$executeRawUnsafe(query);
  }

  private async refreshDailyStats(targetDates: Date[]) {
    if (targetDates.length === 0) return;

    // Convert dates to string format 'YYYY-MM-DD' for the SQL query
    // Example: "'2025-01-01', '2025-01-02'"
    const dateStrings = targetDates
      .map((d) => `'${d.toISOString().split('T')[0]}'`)
      .join(', ');

    const query = `
        INSERT INTO "DailyCsatStat" (
            "date", 
            "totalSurvey", 
            "totalDijawab", 
            "totalJawaban45", 
            "scoreCsat", 
            "persenCsat"
        )
        WITH DailyAggregates AS (
        SELECT 
            DATE("createdAt") as date,
            COUNT(*) as totalSurvey,
            COUNT(CASE WHEN "answeredAt" IS NOT NULL THEN 1 END) as totalDijawab,
            COUNT(CASE WHEN "numeric" >= 4 THEN 1 END) as totalJawaban45
        FROM "RawCsat"
        
        -- Only look at the dates we just inserted/updated
        WHERE DATE("createdAt") IN (${dateStrings}) 
        
        GROUP BY DATE("createdAt")
        ),
        WithPercentage AS (
            SELECT 
            *,
            CASE 
                WHEN totalDijawab = 0 THEN 0
                ELSE (CAST(totalJawaban45 AS FLOAT) / CAST(totalDijawab AS FLOAT)) * 100
            END as calculated_persen
            FROM DailyAggregates
        )
        SELECT 
            date,
            totalSurvey,
            totalDijawab,
            totalJawaban45,
            
            -- 1. Calculate Score from Percentage * 5
            ((calculated_persen/100) * 5) as scoreCsat,
            
            -- 2. The Percentage itself
            calculated_persen as persenCsat

        FROM WithPercentage
        ON CONFLICT ("date") 
        DO UPDATE SET 
            "totalSurvey" = EXCLUDED."totalSurvey",
            "totalDijawab" = EXCLUDED."totalDijawab",
            "totalJawaban45" = EXCLUDED."totalJawaban45",
            "scoreCsat" = EXCLUDED."scoreCsat",
            "persenCsat" = EXCLUDED."persenCsat";
    `;

    await this.prisma.$executeRawUnsafe(query);

    // await this.prisma.$executeRaw`
    //   INSERT INTO "DailyCsatStat" (
    //     "date",
    //     "totalSurvey",
    //     "totalDijawab",
    //     "totalJawaban45",
    //     "scoreCsat",
    //     "persenCsat"
    //   )
    //   WITH DailyAggregates AS (
    //     SELECT
    //       DATE("createdAt") as date,
    //       COUNT(*) as totalSurvey,
    //       COUNT(CASE WHEN "answeredAt" IS NOT NULL THEN 1 END) as totalDijawab,
    //       COUNT(CASE WHEN "numeric" >= 4 THEN 1 END) as totalJawaban45
    //     FROM "RawCsat"
    //     GROUP BY DATE("createdAt")
    //   ),
    //   WithPercentage AS (
    //     SELECT
    //       *,
    //       CASE
    //         WHEN totalDijawab = 0 THEN 0
    //         ELSE (CAST(totalJawaban45 AS FLOAT) / CAST(totalDijawab AS FLOAT)) * 100
    //       END as calculated_persen
    //     FROM DailyAggregates
    //   )
    //   SELECT
    //     date,
    //     totalSurvey,
    //     totalDijawab,
    //     totalJawaban45,

    //     -- 1. Calculate Score from Percentage * 5
    //     ((calculated_persen/100) * 5) as scoreCsat,

    //     -- 2. The Percentage itself
    //     calculated_persen as persenCsat

    //   FROM WithPercentage
    //   ON CONFLICT ("date")
    //   DO UPDATE SET
    //     "totalSurvey" = EXCLUDED."totalSurvey",
    //     "totalDijawab" = EXCLUDED."totalDijawab",
    //     "totalJawaban45" = EXCLUDED."totalJawaban45",
    //     "scoreCsat" = EXCLUDED."scoreCsat",
    //     "persenCsat" = EXCLUDED."persenCsat";
    // `;
  }
}
