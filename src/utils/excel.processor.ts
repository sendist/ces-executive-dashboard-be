import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import * as ExcelJS from 'exceljs';
import { PrismaService } from '../../prisma/prisma.service';
import { parse } from 'path';

@Processor('excel-queue')
export class ExcelProcessor extends WorkerHost {
  constructor(private prisma: PrismaService) {
    super();
  }

  async process(job: Job<any, any, string>): Promise<any> {
    const filePath = job.data.path;
    const batchSize = 1000;
    let rowsToInsert: any[] = [];

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

        const rowData = {
          // id: this.extractFirstId(row.getCell(7).text),
          createdAt: this.parseExcelDate(createdAtRaw), 
          status: row.getCell(3).text,
          // Only parse answeredAt if it exists
          answeredAt: answeredAtRaw ? new Date(answeredAtRaw as string) : null,
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

    // 2. RUN SUMMARIZATION
    await this.refreshDailyStats();
    
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
            reason: 'Duplicate found inside the same Excel batch'
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
          ${this.formatSqlValue(row.createdAt)},
          ${this.formatSqlValue(row.customer)},
          ${this.formatSqlValue(row.status)},
          ${this.formatSqlValue(row.answeredAt)},
          ${this.formatSqlValue(row.ticketNumbers)},
          ${this.formatSqlValue(row.interactionId)},
          ${this.formatSqlValue(row.question1)},
          ${this.formatSqlValue(row.numeric)},
          ${this.formatSqlValue(row.question2)},
          ${this.formatSqlValue(row.question3)},
          ${this.formatSqlValue(row.question4)},
          ${this.formatSqlValue(row.question5)},
          ${this.formatSqlValue(row.question6)},
          ${this.formatSqlValue(row.channel)},
          ${this.formatSqlValue(row.assignedAgent)}
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

    // Helper to safely format values for Raw SQL
  formatSqlValue = (value: any): string => {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (typeof value === 'number') {
      return value.toString();
    }
    if (value instanceof Date) {
      // PostgreSQL expects single quotes for dates: '2025-01-01T10:00:00.000Z'
      return `'${value.toISOString()}'`;
    }
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    // For strings, escape single quotes by doubling them (Standard SQL)
    // e.g. "User's" becomes "User''s"
    const safeString = value.toString().replace(/'/g, "''");
    return `'${safeString}'`;
  };

  private async refreshDailyStats() {
    // This SQL creates your exact JSON requirements
    await this.prisma.$executeRaw`
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
  }

  private parseExcelDate(value) {
      if (!value) return null;

      // CASE 1: Value is a Number (Excel Serial Date)
      // Example: 45658.25 is Jan 1, 2025
      if (typeof value === 'number') {
          // Excel epoch (Dec 30 1899) -> Unix epoch (Jan 1 1970) = 25569 days
          // 86400000 = ms per day
          const date = new Date((value - 25569) * 86400000);
          // Adjust for timezone offset if necessary, but usually UTC is safer
          return date;
      }

      // CASE 2: Value is already a Date object
      if (value instanceof Date) {
          return value;
      }

      // CASE 3: Value is a String ("01/01/2025 06:06:10")
      if (typeof value === 'string') {
          // Split "01/01/2025 06:06:10"
          // Adjust split logic based on your exact format
          const [datePart, timePart] = value.split(' ');
          if (!datePart) return null;

          const [day, month, year] = datePart.split('/');
          const [hour, minute, second] = timePart ? timePart.split(':') : ['00', '00', '00'];

          // Note: Month is 0-indexed in JS (0=Jan)
          return new Date(
              parseInt(year),
              parseInt(month) - 1,
              parseInt(day),
              parseInt(hour || "0"),
              parseInt(minute || "0"),
              parseInt(second || "0")
          );
      }

      // CASE 4: Hyperlinks/Formulas (ExcelJS returns objects sometimes)
      if (typeof value === 'object' && value.result) {
          return this.parseExcelDate(value.result);
      }

      return null;
  }
}