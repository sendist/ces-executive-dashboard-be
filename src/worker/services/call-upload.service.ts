import { Injectable } from '@nestjs/common';
import { Job } from 'bullmq';
import { PrismaService } from 'prisma/prisma.service';
import * as ExcelJS from 'exceljs';
import { ExcelUtils } from '../excel-utils.helper';
import * as fs from 'fs';

@Injectable()
export class CallUploadService {
  constructor(private readonly prisma: PrismaService) {}

  async process(job: Job) {
    const filePath = job.data.path;
    if (!fs.existsSync(filePath)) {
      console.error(`File missing at path: ${filePath}`);
      // Throwing an error here marks the job as FAILED in BullMQ,
      // but it won't crash your entire Node.js server.
      throw new Error(`File not found: ${filePath} - likely a stale job.`);
    }
    const kipMap = await this.createLookupMap(
      this.prisma.lookupKIP,
      'compositeKey',
      'product',
    );

    const accountMap = await this.createLookupMap(
      this.prisma.accountMapping,
      'corporateName',
      'kategoriAccount',
    );

    const batchSize = 1000;
    let rowsToInsert: any[] = [];

    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});

    for await (const worksheet of workbook) {
      for await (const row of worksheet) {
        if (row.number === 1) continue; // Skip Header

        // Get the raw notes text
        const rawNotes = row.getCell(16).text;

        // Extract the specific fields
        const extractedData = this.extractNotesData(rawNotes);

        const rawNamaPerusahaan = extractedData.corp;
        const normalizedNamaPerusahaan =
          typeof rawNamaPerusahaan === 'string'
            ? rawNamaPerusahaan.trim().toLowerCase()
            : '';
        const derivedAccountCategory = accountMap.get(
          normalizedNamaPerusahaan || '',
        );

        const compositeFcrKey =
          `${row.getCell(11).text}_${row.getCell(8).text}_${row.getCell(9).text}`
            .trim()
            .toLowerCase();
        // const fcrStatus = fcrMap.get(compositeFcrKey) || false;

        const derivedProduct = kipMap.get(compositeFcrKey || '-');

        const rowData = {
          updateStamp: ExcelUtils.parseExcelDate(row.getCell(1).value),
          msisdn: row.getCell(2).text,
          brand: row.getCell(3).text,
          unitType: row.getCell(4).text,
          unitName: row.getCell(5).text,
          areaName: row.getCell(6).text,
          regName: row.getCell(7).text,
          topicReason1: row.getCell(8).text,
          topicReason2: row.getCell(9).text,
          topicResult: row.getCell(10).text,
          service: row.getCell(11).text,
          appId: row.getCell(12).text,
          userId: row.getCell(13).text,
          employeeCode: row.getCell(14).text,
          employeeName: row.getCell(15).text,
          notes: row.getCell(16).text, // Long text field

          corp: extractedData.corp,
          projectId: extractedData.projectId,
          tier: extractedData.tier,
          customerType: extractedData.customerType,

          // row tambahan
          validationStatus: 'valid',
          statusTiket: true,
          product: derivedProduct?.toUpperCase() || '-',
          sla: true,
          fcr: false,
          eskalasi: '-',
          isPareto: derivedAccountCategory === 'P1' ? true : false,
          isVip: false,
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
    const values = cleanRows
      .map((row) => {
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
        ${ExcelUtils.formatSqlValue(row.notes)},
        ${ExcelUtils.formatSqlValue(row.corp)},
        ${ExcelUtils.formatSqlValue(row.projectId)},
        ${ExcelUtils.formatSqlValue(row.tier)},
        ${ExcelUtils.formatSqlValue(row.customerType)},
        ${ExcelUtils.formatSqlValue(row.validationStatus)},
        ${ExcelUtils.formatSqlValue(row.statusTiket)},
        ${ExcelUtils.formatSqlValue(row.product)},
        ${ExcelUtils.formatSqlValue(row.sla)},
        ${ExcelUtils.formatSqlValue(row.fcr)},
        ${ExcelUtils.formatSqlValue(row.eskalasi)},
        ${ExcelUtils.formatSqlValue(row.isPareto)},
        ${ExcelUtils.formatSqlValue(row.isVip)}
        )`;
      })
      .join(',');

    // 3. EXECUTE UPSERT
    // We update the other fields if a conflict is found, so the data stays fresh.
    const query = `
        INSERT INTO "RawCall" (
        "update_stamp", "msisdn", "brand", "unit_type", "unit_name",
        "area_name", "reg_name", "topic_reason_1", "topic_reason_2",
        "topic_result", "service", "app_id", "user_id",
        "employee_code", "employee_name", "notes", "corp", "project_id",
        "tier", "customer_type", "validationStatus", "statusTiket",
        "product", "inSla", "isFcr", "eskalasi", "isPareto", "isVip"
        )
        VALUES ${values}
        ON CONFLICT ("update_stamp", "msisdn")
        DO UPDATE SET
        "brand"           = EXCLUDED."brand",           
        "unit_type"       = EXCLUDED."unit_type",
        "unit_name"       = EXCLUDED."unit_name",
        "area_name"       = EXCLUDED."area_name",
        "reg_name"        = EXCLUDED."reg_name",
        "topic_reason_1"  = EXCLUDED."topic_reason_1",
        "topic_reason_2"  = EXCLUDED."topic_reason_2",
        "topic_result"    = EXCLUDED."topic_result",
        "service"         = EXCLUDED."service",
        "app_id"          = EXCLUDED."app_id",
        "user_id"         = EXCLUDED."user_id",
        "employee_code"   = EXCLUDED."employee_code",
        "employee_name"   = EXCLUDED."employee_name",
        "notes"           = EXCLUDED."notes",
        "corp"            = EXCLUDED."corp",
        "project_id"      = EXCLUDED."project_id",
        "tier"            = EXCLUDED."tier",
        "customer_type"   = EXCLUDED."customer_type",
        "validationStatus"= EXCLUDED."validationStatus",
        "statusTiket"     = EXCLUDED."statusTiket",
        "product"         = EXCLUDED."product",
        "inSla"           = EXCLUDED."inSla",
        "isFcr"           = EXCLUDED."isFcr",
        "eskalasi"        = EXCLUDED."eskalasi",
        "isPareto"        = EXCLUDED."isPareto",
        "isVip"           = EXCLUDED."isVip";
    `;

    await this.prisma.$executeRawUnsafe(query);
  }

  private extractNotesData(notes: string) {
    // Normalize notes to avoid issues with different newline characters
    const cleanNotes = notes || '';

    // 1. Extract Corp Name
    // Looks for "Corp :" followed by text until the end of the line
    const corpMatch = cleanNotes.match(/Corp\s*:\s*(.*)/i);
    let corp = corpMatch ? corpMatch[1].trim() : null;

    // SAFETY CHECK:
    // If the extracted 'corp' accidentally contains the next field's label
    // (e.g. if the file was formatted as "Corp : Project ID: 123"), clean it.
    if (corp && /Project\s*ID/i.test(corp)) {
      corp = null;
    }

    // 2. Extract Project ID
    // Looks for "Project ID :" followed by text
    const projectIdMatch = cleanNotes.match(/Project\s*ID\s*:\s*(.*)/i);
    const projectId = projectIdMatch ? projectIdMatch[1].trim() : null;

    // 3. Extract Tags (Tier and Category)
    // We scan the whole text for anything inside square brackets []
    // because they usually appear at the start: [CORP][SILVER] or [POST][Regular]
    const allTags = (cleanNotes.match(/\[(.*?)\]/g) || []).map((tag) =>
      tag
        .replace(/[\[\]]/g, '')
        .toUpperCase()
        .trim(),
    );

    // Detect Tier based on known keywords found in the extracted tags
    const knownTiers = ['SILVER', 'GOLD', 'PLATINUM', 'DIAMOND'];
    const tier = allTags.find((t) => knownTiers.includes(t)) || null;

    // Detect Category (CORP vs REGULAR)
    // We look for 'CORP' or 'REGULAR'. If 'POST' appears, you might treat it as Regular
    // depending on your logic, but here we explicitly look for the requested keywords.
    let customerType = '';
    if (allTags.includes('CORP')) customerType = 'CORP';
    else if (allTags.includes('REGULAR')) customerType = 'REGULAR';
    // Fallback: If no explicit tag, sometimes logic dictates checking the Corp field exists
    if (!customerType && corp) customerType = 'CORP';

    return {
      corp,
      projectId,
      tier,
      customerType,
    };
  }

  private async createLookupMap(
    modelDelegate: any,
    keyField: string,
    valueField: string,
  ): Promise<Map<string, string>> {
    // 1. Dynamic Select: Fetch only the columns we need
    const data = await modelDelegate.findMany({
      select: {
        [keyField]: true,
        [valueField]: true,
      },
    });

    // 2. Build Map with normalization
    const lookupMap = new Map<string, string>();

    for (const row of data) {
      const rawKey = row[keyField];
      const value = row[valueField];

      // Ensure key exists and is a string before processing
      if (rawKey && typeof rawKey === 'string') {
        lookupMap.set(rawKey.trim().toLowerCase(), value || '');
      }
    }

    return lookupMap;
  }
}
