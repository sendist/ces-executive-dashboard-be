/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { Injectable } from "@nestjs/common";
import { PrismaService } from "prisma/prisma.service";
import { Job } from 'bullmq';
import csv from 'csv-parser';
import * as fs from 'fs';
import { ExcelUtils } from "../excel-utils.helper";
import { calculateFcrStatus, calculateSlaStatus, determineEskalasi, TICKET_RULES } from "../utils/rules.constant";

@Injectable()
export class OcaUploadService {
  constructor(
      private readonly prisma: PrismaService,
  ){}
  // regex to identify VIP keywords
  private readonly vipRegex = /vvip|vip|direk|director|komisaris/i;

  async process(job: Job) {

    const kipMap = await this.createLookupMap(
      this.prisma.kIP,
      'subCategory',
      'product'
    );

    const accountMap = await this.createLookupMap(
      this.prisma.accountMapping,
      'corporateName',
      'kategoriAccount',
    );

    const filePath = job.data.path;
    if (!fs.existsSync(filePath)) {
        console.error(`File missing at path: ${filePath}`);
        // Throwing an error here marks the job as FAILED in BullMQ,
        // but it won't crash your entire Node.js server.
        throw new Error(`File not found: ${filePath} - likely a stale job.`);
    }

    const batchSize = 1000;
    let rowsToInsert: any[] = [];

    const separator = this.detectDelimiter(filePath);

    // Create a stream that pipes the file through the CSV parser
    const stream = fs.createReadStream(filePath).pipe(csv({
      separator,
      mapHeaders: ({ header }) => header.trim() // Safely trim whitespace/BOM from headers
    }));

    // Async Iterator: This reads the CSV line by line without loading it all into RAM
    for await (const row of stream) {
      const classification = this.classifyTicket(row);

      const rawSubCategory = row['Sub Category'];
      const normalizedSubCategory =
        typeof rawSubCategory === 'string'
          ? rawSubCategory.trim().toLowerCase()
          : '';
      const derivedProduct = kipMap.get(normalizedSubCategory || '');

      const rawNamaPerusahaan = row['Nama Perusahaan'];
      const normalizedNamaPerusahaan =
        typeof rawNamaPerusahaan === 'string'
          ? rawNamaPerusahaan.trim().toLowerCase()
          : '';
      const derivedAccountCategory = accountMap.get(normalizedNamaPerusahaan || '');

      const ticketSubject = row['Ticket Subject'] || '';
      const isVip = this.vipRegex.test(ticketSubject);
      
      // --- 2. RUN SLA CALCULATION ---
      // Now we pass the 'derivedProduct' as 'Kolom BF'
      const slaStatus = calculateSlaStatus({
          'product': derivedProduct, 
          'ticketCreated': row['Ticket Created'],
          'resolveTime': row['Resolve Time']
      });

      const fcrStatus = calculateFcrStatus({
        'ID Remedy_NO': row['ID Remedy_NO'],
        'Eskalasi/ID Remedy_IT/AO/EMS': row['Eskalasi/ID Remedy_IT/AO/EMS'],
        'Jumlah MSISDN': row['Jumlah MSISDN']
      });

      const typeEskalasi = determineEskalasi({
        'ID Remedy_NO': row['ID Remedy_NO'], 
        'Eskalasi/ID Remedy_IT/AO/EMS': row['Eskalasi/ID Remedy_IT/AO/EMS']
      })

      const rowData = {
        // EXACT header string from CSV
        ticketNumber:    row['Ticket Number'],
        ticketSubject:   row['Ticket Subject'],
        channel:         row['Channel'],
        category:        row['Category'],
        reporter:        row['Reporter'],
        assignee:        row['Assignee'],
        department:      row['Department'],
        priority:        row['Priority'],
        lastStatus:      row['Last Status'],
        
        // Date Parsing
        ticketCreated:   ExcelUtils.parseExcelDate(row['Ticket Created']),
        lastUpdate:      ExcelUtils.parseExcelDate(row['Last Update']),
        
        description:     row['Description'],
        customerName:    row['Customer Name'],
        customerPhone:   row['Customer Phone'],
        customerAddress: row['Customer Address'],
        customerEmail:   row['Customer Email'],
        
        firstResponseTime:   ExcelUtils.parseExcelDate(row['First Response Time']),
        totalResponseTime:   row['Total Response Time'],
        totalResolutionTime: row['Total Resolution Time'],
        resolveTime:         ExcelUtils.parseExcelDate(row['Resolve Time']),
        resolvedBy:          row['Resolved By'],
        closedTime:          ExcelUtils.parseExcelDate(row['Closed Time']),
        ticketDuration:      row['Ticket Duration'],
        
        // Number Parsing
        countInboundMessage: ExcelUtils.parseSafeInt(row['Count Inbound Message']),
        labelInRoom:         row['Label In Room'],
        firstResponseDuration: row['First Response Duration'],
        
        escalateTicket:        row['Escalate Ticket'],
        lastAssigneeEscalation: row['Last Assignee Escalation'],
        lastStatusEscalation:   row['Last Status Escalation'],
        lastUpdateEscalation:   row['Last Update Escalation'],
        
        converse:           row['Converse'],
        moveToOtherChannel: row['Move to other channel'],
        previousChannel:    row['Previous channel'],
        
        amountRevenue: ExcelUtils.parseSafeBigInt(row['Amount Revenue']),
        jumlahMsisdn:  row['Jumlah MSISDN'],
        
        tags:          row['Tags'],
        idRemedyNo:    row['ID Remedy_NO'],
        eskalasiId:    row['Eskalasi/ID Remedy_IT/AO/EMS'], 
        reasonOsl:     row['Reason OSL'],
        projectId:     row['Project ID'],
        namaPerusahaan: row['Nama Perusahaan'],
        roaming:       row['Roaming'],
        subCategory:   row['Sub Category'],
        detailCategory: row['Detail Category'],
        iot:           row['IOT'],

        // row tambahan
        validationStatus: classification.status,
        statusTiket:      classification.isValid,
        product:          derivedProduct,
        sla:              slaStatus,
        fcr:              fcrStatus,
        eskalasi:         typeEskalasi,
        isPareto:         derivedAccountCategory === 'P1' ? true : false,
        isVip:            isVip,
        
        updatedAtExcel: ExcelUtils.parseExcelDate(row['Updated at'])
      };

      rowsToInsert.push(rowData);

      // If batch is full, pause stream, save to DB, then resume
      if (rowsToInsert.length >= batchSize) {
        await this.saveBatch(rowsToInsert);
        rowsToInsert = [];
      }
    }

    // Save remaining rows
    if (rowsToInsert.length > 0) {
      await this.saveBatch(rowsToInsert);
    }

    return { status: 'CSV Ticket Report Completed' };
  }

  private async saveBatch(rows: any[]) {
    if (rows.length === 0) return;

    // 1. DEDUPLICATE IN MEMORY
    const uniqueRowsMap = new Map<string, any>();
    for (const row of rows) {
      if (!row.ticketNumber) continue;
      uniqueRowsMap.set(row.ticketNumber, row); 
    }

    const cleanRows = Array.from(uniqueRowsMap.values());
    if (cleanRows.length === 0) return;

    // 2. BUILD SQL VALUES (Order must match INSERT columns below)
    const values = cleanRows.map((row) => {
      return `(
        ${ExcelUtils.formatSqlValue(row.ticketNumber)},
        ${ExcelUtils.formatSqlValue(row.ticketSubject)},
        ${ExcelUtils.formatSqlValue(row.channel)},
        ${ExcelUtils.formatSqlValue(row.category)},
        ${ExcelUtils.formatSqlValue(row.reporter)},
        ${ExcelUtils.formatSqlValue(row.assignee)},
        ${ExcelUtils.formatSqlValue(row.department)},
        ${ExcelUtils.formatSqlValue(row.priority)},
        ${ExcelUtils.formatSqlValue(row.lastStatus)},
        ${ExcelUtils.formatSqlValue(row.ticketCreated)},
        ${ExcelUtils.formatSqlValue(row.lastUpdate)},
        ${ExcelUtils.formatSqlValue(row.description)},
        ${ExcelUtils.formatSqlValue(row.customerName)},
        ${ExcelUtils.formatSqlValue(row.customerPhone)},
        ${ExcelUtils.formatSqlValue(row.customerAddress)},
        ${ExcelUtils.formatSqlValue(row.customerEmail)},
        ${ExcelUtils.formatSqlValue(row.firstResponseTime)},
        ${ExcelUtils.formatSqlValue(row.totalResponseTime)},
        ${ExcelUtils.formatSqlValue(row.totalResolutionTime)},
        ${ExcelUtils.formatSqlValue(row.resolveTime)},
        ${ExcelUtils.formatSqlValue(row.resolvedBy)},
        ${ExcelUtils.formatSqlValue(row.closedTime)},
        ${ExcelUtils.formatSqlValue(row.ticketDuration)},
        ${ExcelUtils.formatSqlValue(row.countInboundMessage)},
        ${ExcelUtils.formatSqlValue(row.labelInRoom)},
        ${ExcelUtils.formatSqlValue(row.firstResponseDuration)},
        ${ExcelUtils.formatSqlValue(row.escalateTicket)},
        ${ExcelUtils.formatSqlValue(row.lastAssigneeEscalation)},
        ${ExcelUtils.formatSqlValue(row.lastStatusEscalation)},
        ${ExcelUtils.formatSqlValue(row.lastUpdateEscalation)},
        ${ExcelUtils.formatSqlValue(row.converse)},
        ${ExcelUtils.formatSqlValue(row.moveToOtherChannel)},
        ${ExcelUtils.formatSqlValue(row.previousChannel)},
        ${ExcelUtils.formatSqlValue(row.amountRevenue)},
        ${ExcelUtils.formatSqlValue(row.jumlahMsisdn)},
        ${ExcelUtils.formatSqlValue(row.tags)},
        ${ExcelUtils.formatSqlValue(row.idRemedyNo)},
        ${ExcelUtils.formatSqlValue(row.eskalasiId)},
        ${ExcelUtils.formatSqlValue(row.reasonOsl)},
        ${ExcelUtils.formatSqlValue(row.projectId)},
        ${ExcelUtils.formatSqlValue(row.namaPerusahaan)},
        ${ExcelUtils.formatSqlValue(row.roaming)},
        ${ExcelUtils.formatSqlValue(row.subCategory)},
        ${ExcelUtils.formatSqlValue(row.detailCategory)},
        ${ExcelUtils.formatSqlValue(row.iot)},
        ${ExcelUtils.formatSqlValue(row.validationStatus)},
        ${ExcelUtils.formatSqlValue(row.statusTiket)},
        ${ExcelUtils.formatSqlValue(row.product)},
        ${ExcelUtils.formatSqlValue(row.sla)},
        ${ExcelUtils.formatSqlValue(row.fcr)},
        ${ExcelUtils.formatSqlValue(row.eskalasi)},
        ${ExcelUtils.formatSqlValue(row.isVip)},
        ${ExcelUtils.formatSqlValue(row.isPareto)},
        ${ExcelUtils.formatSqlValue(row.updatedAtExcel)}

      )`;
    }).join(',');

    // 3. EXECUTE QUERY with SNAKE_CASE columns
    const query = `
      INSERT INTO "RawOca" (
        "ticket_number", "ticket_subject", "channel", "category", 
        "reporter", "assignee", "department", "priority", "last_status",
        "ticket_created", "last_update", "description", 
        "customer_name", "customer_phone", "customer_address", "customer_email",
        "first_response_time", "total_response_time", "total_resolution_time",
        "resolve_time", "resolved_by", "closed_time", "ticket_duration",
        "count_inbound_message", "label_in_room", "first_response_duration",
        "escalate_ticket", "last_assignee_escalation", "last_status_escalation",
        "last_update_escalation", "converse", "move_to_other_channel", "previous_channel",
        "amount_revenue", "jumlah_msisdn", "tags", "id_remedy_no",
        "eskalasi_id_remedy_it_ao_ems", "reason_osl", "project_id", "nama_perusahaan",
        "roaming", "sub_category", "detail_category", "iot", "validationStatus", "statusTiket", "product",
        "inSla", "isFcr", "eskalasi", "isVip", "isPareto", "updated_at_excel"
      )
      VALUES ${values}
      ON CONFLICT ("ticket_number")
      DO UPDATE SET
        "ticket_subject" = EXCLUDED."ticket_subject",
        "channel"        = EXCLUDED."channel",
        "category"       = EXCLUDED."category",
        "assignee"       = EXCLUDED."assignee",
        "last_status"    = EXCLUDED."last_status",
        "last_update"    = EXCLUDED."last_update",
        "description"    = EXCLUDED."description",
        "resolved_by"    = EXCLUDED."resolved_by",
        "closed_time"    = EXCLUDED."closed_time",
        "validationStatus" = EXCLUDED."validationStatus",
        "statusTiket"    = EXCLUDED."statusTiket",
        "inSla"            = EXCLUDED."inSla",
        "updated_at_excel" = EXCLUDED."updated_at_excel";
    `;

    await this.prisma.$executeRawUnsafe(query);
  }

  private classifyTicket(row: any) {
    // 1. Iterate through defined rules
    for (const rule of TICKET_RULES) {
        // Get value safely (handle casing if needed)
        const cellValue = row[rule.column]; 
        
        // If rule matches, return that status immediately (Fail-Fast)
        if (cellValue && rule.check(cellValue)) {
            return { 
                status: rule.status, 
                isValid: false, // It hit a "Double/EMS/RPA" rule
                reason: `Matched ${rule.status} rule on ${rule.column}` 
            };
        }
    }

    // 2. Special Case: The "Valid" Description override from your image
    // If the image implies "completed by hia" overrides others, put this BEFORE the loop.
    // If it implies "it's valid if it contains this", we handle it here as a fallback.
    if (row['description'] && /completed by hia/i.test(row['description'])) {
         return { status: 'Valid', isValid: true, reason: 'Completed by HIA' };
    }

    // 3. Default Fallback (Row 11 in your image)
    return { status: 'Valid', isValid: true, reason: 'Passed all checks' };
  }

  /**
   * Generic helper to fetch reference data and create a normalized Map
   * @param modelDelegate The prisma model (e.g. this.prisma.kIP)
   * @param keyField The database column to be used as the Map Key (normalized)
   * @param valueField The database column to be used as the Map Value
   */
  private async createLookupMap(
    modelDelegate: any, 
    keyField: string, 
    valueField: string
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

  detectDelimiter(filePath) {
    const fd = fs.openSync(filePath, 'r');
    const buffer = Buffer.alloc(1024); // enough to read header line
    fs.readSync(fd, buffer, 0, buffer.length, 0);
    fs.closeSync(fd);

    const firstLine = buffer.toString('utf8').split('\n')[0];

    const commaCount = (firstLine.match(/,/g) || []).length;
    const semicolonCount = (firstLine.match(/;/g) || []).length;

    return semicolonCount > commaCount ? ';' : ',';
  }

}