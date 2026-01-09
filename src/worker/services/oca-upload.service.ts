import { Injectable } from "@nestjs/common";
import { PrismaService } from "prisma/prisma.service";
import { Job } from 'bullmq';
import csv from 'csv-parser';
import * as fs from 'fs';
import { ExcelUtils } from "../excel-utils.helper";

@Injectable()
export class OcaUploadService {
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

    // Create a stream that pipes the file through the CSV parser
    const stream = fs.createReadStream(filePath).pipe(csv({
      mapHeaders: ({ header }) => header.trim() // Safely trim whitespace/BOM from headers
    }));

    // Async Iterator: This reads the CSV line by line without loading it all into RAM
    for await (const row of stream) {
      
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
        "roaming", "sub_category", "detail_category", "iot", "updated_at_excel"
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
        "updated_at_excel" = EXCLUDED."updated_at_excel";
    `;

    await this.prisma.$executeRawUnsafe(query);
  }

}