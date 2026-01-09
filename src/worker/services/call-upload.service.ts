import { Injectable } from "@nestjs/common";
import { Job } from "bullmq";
import { PrismaService } from "prisma/prisma.service";
import * as ExcelJS from 'exceljs';
import { ExcelUtils } from "../excel-utils.helper";
import * as fs from 'fs';

@Injectable()
export class CallUploadService {
    constructor(
        private readonly prisma: PrismaService,
    ){}

  async process(job: Job) {
    const filePath = job.data.path;
    if (!fs.existsSync(filePath)) {
        console.error(`File missing at path: ${filePath}`);
        // Throwing an error here marks the job as FAILED in BullMQ,
        // but it won't crash your entire Node.js server.
        throw new Error(`File not found: ${filePath} - likely a stale job.`);
    }

    const batchSize = 1000;
    let rowsToInsert: any[] = [];

    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});

    for await (const worksheet of workbook) {
        for await (const row of worksheet) {
        if (row.number === 1) continue; // Skip Header

        const rowData = {
            updateStamp:  ExcelUtils.parseExcelDate(row.getCell(1).value),
            msisdn:       row.getCell(2).text,
            brand:        row.getCell(3).text,
            unitType:     row.getCell(4).text,
            unitName:     row.getCell(5).text,
            areaName:     row.getCell(6).text,
            regName:      row.getCell(7).text,
            topicReason1: row.getCell(8).text,
            topicReason2: row.getCell(9).text,
            topicResult:  row.getCell(10).text,
            service:      row.getCell(11).text,
            appId:        row.getCell(12).text,
            userId:       row.getCell(13).text,
            employeeCode: row.getCell(14).text,
            employeeName: row.getCell(15).text,
            notes:        row.getCell(16).text, // Long text field
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

    return { status: 'Raw Call Completed' };
    }

    private async saveBatch(rows: any[]) {
    if (rows.length === 0) return;

    // 1. DEDUPLICATE IN MEMORY
    // Key: "timestamp_msisdn"
    const uniqueRowsMap = new Map<string, any>();
    
    for (const row of rows) {
        // Safety: If either key is missing, we can't enforce uniqueness, so skip or allow
        if (!row.updateStamp || !row.msisdn) continue;

        const uniqueKey = `${row.updateStamp.getTime()}_${row.msisdn}`;
        
        // Overwrite ensures the latest row in the file wins
        uniqueRowsMap.set(uniqueKey, row);
    }

    const cleanRows = Array.from(uniqueRowsMap.values());
    if (cleanRows.length === 0) return;

    // 2. BUILD SQL VALUES
    const values = cleanRows.map((row) => {
        return `(
        ${ExcelUtils.formatSqlValue(row.updateStamp)},
        ${ExcelUtils.formatSqlValue(row.msisdn)},
        ${ExcelUtils.formatSqlValue(row.brand)},
        ${ExcelUtils.formatSqlValue(row.unitType)},
        ${ExcelUtils.formatSqlValue(row.unitName)},
        ${ExcelUtils.formatSqlValue(row.areaName)},
        ${ExcelUtils.formatSqlValue(row.regName)},
        ${ExcelUtils.formatSqlValue(row.topicReason1)},
        ${ExcelUtils.formatSqlValue(row.topicReason2)},
        ${ExcelUtils.formatSqlValue(row.topicResult)},
        ${ExcelUtils.formatSqlValue(row.service)},
        ${ExcelUtils.formatSqlValue(row.appId)},
        ${ExcelUtils.formatSqlValue(row.userId)},
        ${ExcelUtils.formatSqlValue(row.employeeCode)},
        ${ExcelUtils.formatSqlValue(row.employeeName)},
        ${ExcelUtils.formatSqlValue(row.notes)}
        )`;
    }).join(',');

    // 3. EXECUTE UPSERT
    // We update the other fields if a conflict is found, so the data stays fresh.
    const query = `
        INSERT INTO "RawCall" (
        "Update_Stamp", "MSISDN", "BRAND", "UNIT_TYPE", "UNIT_NAME",
        "AREA_NAME", "REG_NAME", "TOPIC_REASON_1", "TOPIC_REASON_2",
        "TOPIC_RESULT", "SERVICE", "APP_ID", "USER_ID",
        "EMPLOYEE_CODE", "EMPLOYEE_NAME", "NOTES"
        )
        VALUES ${values}
        ON CONFLICT ("Update_Stamp", "MSISDN")
        DO UPDATE SET
        "BRAND"           = EXCLUDED."BRAND",
        "UNIT_TYPE"       = EXCLUDED."UNIT_TYPE",
        "UNIT_NAME"       = EXCLUDED."UNIT_NAME",
        "AREA_NAME"       = EXCLUDED."AREA_NAME",
        "REG_NAME"        = EXCLUDED."REG_NAME",
        "TOPIC_REASON_1"  = EXCLUDED."TOPIC_REASON_1",
        "TOPIC_REASON_2"  = EXCLUDED."TOPIC_REASON_2",
        "TOPIC_RESULT"    = EXCLUDED."TOPIC_RESULT",
        "SERVICE"         = EXCLUDED."SERVICE",
        "APP_ID"          = EXCLUDED."APP_ID",
        "USER_ID"         = EXCLUDED."USER_ID",
        "EMPLOYEE_CODE"   = EXCLUDED."EMPLOYEE_CODE",
        "EMPLOYEE_NAME"   = EXCLUDED."EMPLOYEE_NAME",
        "NOTES"           = EXCLUDED."NOTES";
    `;
    
    await this.prisma.$executeRawUnsafe(query);
    }
}