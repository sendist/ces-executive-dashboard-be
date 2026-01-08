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

  // Traffic Controller
  async process(job: Job<any, any, string>): Promise<any> {
    switch (job.name) {
      case 'process-csat-report':
        return this.processCsatFile(job);
      case 'process-omnix-report':
        return this.processOmnixFile(job);
      case 'process-call-report':
        return this.processCallFile(job);
      default:
        throw new Error(`Unknown job name: ${job.name}`);
    }
  }

  // Call this from your main process() switch case
  private async processCallFile(job: Job) {
    const filePath = job.data.path;
    const batchSize = 1000;
    let rowsToInsert: any[] = [];

    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});

    for await (const worksheet of workbook) {
      for await (const row of worksheet) {
        if (row.number === 1) continue; // Skip Header

        const rowData = {
          updateStamp:  this.parseExcelDate(row.getCell(1).value),
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
          await this.saveCallBatch(rowsToInsert);
          rowsToInsert = [];
        }
      }
    }

    if (rowsToInsert.length > 0) {
      await this.saveCallBatch(rowsToInsert);
    }

    return { status: 'Raw Call Completed' };
  }

  private async saveCallBatch(rows: any[]) {
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
        ${this.formatSqlValue(row.updateStamp)},
        ${this.formatSqlValue(row.msisdn)},
        ${this.formatSqlValue(row.brand)},
        ${this.formatSqlValue(row.unitType)},
        ${this.formatSqlValue(row.unitName)},
        ${this.formatSqlValue(row.areaName)},
        ${this.formatSqlValue(row.regName)},
        ${this.formatSqlValue(row.topicReason1)},
        ${this.formatSqlValue(row.topicReason2)},
        ${this.formatSqlValue(row.topicResult)},
        ${this.formatSqlValue(row.service)},
        ${this.formatSqlValue(row.appId)},
        ${this.formatSqlValue(row.userId)},
        ${this.formatSqlValue(row.employeeCode)},
        ${this.formatSqlValue(row.employeeName)},
        ${this.formatSqlValue(row.notes)}
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

  async processOmnixFile(job: Job<any, any, string>): Promise<any> {
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

        // Helper to safely parse Integers (returns null if empty or invalid)
        const parseIntSafe = (value: any) => {
          const parsed = parseInt(value);
          return isNaN(parsed) ? null : parsed;
        };

        const parseJsonSafe = (value: any) => {
          if (!value) return null;
          try {
            return typeof value === 'object' ? value : JSON.parse(value);
          } catch (e) {
            return null; // or return value if you want to save as plain string
          }
        };

        const rowData = {
          // --- 1. Basic Ticket Info ---
          ticketId:         parseIntSafe(row.getCell(1).value),
          remark:           row.getCell(2).text,
          subject:          row.getCell(3).text,
          priorityId:       parseIntSafe(row.getCell(4).value),
          priorityName:     row.getCell(5).text,
          ticketStatusId:   parseIntSafe(row.getCell(6).value),
          ticketStatusName: row.getCell(7).text,
          unitId:           parseIntSafe(row.getCell(8).value),
          unitName:         row.getCell(9).text,

          // --- 2. Informant & Customer ---
          informantId:      row.getCell(10).text,
          informantName:    row.getCell(11).text,
          informantHp:      row.getCell(12).text,
          informantEmail:   row.getCell(13).text,
          customerId:       row.getCell(14).text,
          customerName:     row.getCell(15).text,
          customerHp:       row.getCell(16).text,
          customerEmail:    row.getCell(17).text,

          // --- 3. Interaction Dates ---
          dateOriginInteraction:      this.parseExcelDate(row.getCell(18).value),
          dateStartInteraction:       this.parseExcelDate(row.getCell(19).value),
          dateOpen:                   this.parseExcelDate(row.getCell(20).value),
          dateClose:                  this.parseExcelDate(row.getCell(21).value),
          dateLastUpdate:             this.parseExcelDate(row.getCell(22).value),

          // --- 4. Categorization ---
          isEscalated:   row.getCell(23).text,
          createdById:   parseIntSafe(row.getCell(24).value),
          createdByName: row.getCell(25).text,
          updatedById:   parseIntSafe(row.getCell(26).value),
          updatedByName: row.getCell(27).text,
          channelId:     parseIntSafe(row.getCell(28).value),
          sessionId:     row.getCell(29).text,
          categoryId:    parseIntSafe(row.getCell(30).value),
          categoryName:  row.getCell(31).text,
          dateCreatedAt: this.parseExcelDate(row.getCell(32).value),

          // --- 5. Details ---
          sla:                row.getCell(33).text,
          channelName:        row.getCell(34).text,
          mainCategory:       row.getCell(35).text,
          category:           row.getCell(36).text,
          subCategory:        row.getCell(37).text,
          detailSubCategory:  row.getCell(38).text,
          detailSubCategory2: row.getCell(39).text,

          // --- 6. More Dates ---
          datePickupInteraction:        this.parseExcelDate(row.getCell(40).value),
          dateEndInteraction:           this.parseExcelDate(row.getCell(41).value),
          dateFirstPickupInteraction:   this.parseExcelDate(row.getCell(42).value),
          dateFirstResponseInteraction: this.parseExcelDate(row.getCell(43).value),

          // --- 7. Account & Sentiment ---
          account:           row.getCell(44).text,
          accountName:       row.getCell(45).text,
          informantMemberId: row.getCell(46).text,
          customerMemberId:  row.getCell(47).text,
          sentimentIncoming: row.getCell(48).text,
          sentimentOutgoing: row.getCell(49).text,
          sentimentAll:      row.getCell(50).text,
          feedback:          row.getCell(51).text,
          sentimentService:  row.getCell(52).text,

          // --- 8. Merging & Source ---
          parentId:    row.getCell(53).text,
          countMerged: parseIntSafe(row.getCell(54).value),
          sourceId:    parseIntSafe(row.getCell(55).value),
          sourceName:  row.getCell(56).text,

          // --- 9. JSON Data ---
          contact: parseJsonSafe(row.getCell(57).text),

          // --- 10. Survey & Additional ---
          surveyName:                row.getCell(58).text,
          interactionAdditionalInfo: parseJsonSafe(row.getCell(59).text), // Assuming this is also JSON
          surveyId:                  row.getCell(60).text,
          respondentId:              row.getCell(61).text,
          ticketIdOld:               row.getCell(62).text,

          // --- 11. Durations ---
          waitingTime:  row.getCell(63).text,
          serviceTime:  row.getCell(64).text,
          responseTime: row.getCell(65).text,
          handlingTime: row.getCell(66).text,
          duration:     row.getCell(67).text,
          acw:          row.getCell(68).text,

          // --- 12. Specific Custom Fields ---
          ticketPerusahaan:  row.getCell(69).text,
          ticketAmount:      row.getCell(70).text,
          ticketRemedyNo:    row.getCell(71).text,
          ticketITAO:        row.getCell(72).text, // Mapped from "ticket_IT/AO"
          ticketProject:     row.getCell(73).text,
          slaSecond:         parseIntSafe(row.getCell(74).value),
          ticketIdMasking:   row.getCell(75).text,
          informantNamaCorp: row.getCell(76).text,
          customerNamaCorp:  row.getCell(77).text,

          // --- 13. Escalation Dates ---
          datePending:     this.parseExcelDate(row.getCell(78).value),
          dateResolve:     this.parseExcelDate(row.getCell(79).value),
          dateEskalasiEbo: this.parseExcelDate(row.getCell(80).value),
          dateEskalasiIt:  this.parseExcelDate(row.getCell(81).value),
          dateEskalasiNo:  this.parseExcelDate(row.getCell(82).value),
          dateEskalasi:    this.parseExcelDate(row.getCell(83).value),

          // --- 14. Final Fields ---
          partner:             row.getCell(84).text,
          dateMenunggu:        this.parseExcelDate(row.getCell(85).value),
          approvalBillco:      row.getCell(86).text,
          customerInstagramId: row.getCell(87).text,
          customerPhone:       row.getCell(88).text,
          customerFacebookId:  row.getCell(89).text,
        };

        rowsToInsert.push(rowData);

        if (rowsToInsert.length >= batchSize) {
          await this.saveOmnixBatch(rowsToInsert);
          rowsToInsert = [];
        }
      }
    }

    if (rowsToInsert.length > 0) {
      await this.saveOmnixBatch(rowsToInsert);
    }

    // 2. RUN SUMMARIZATION
    // await this.refreshDailyStats();
    
    return { status: 'Completed' };

  }

  private async saveOmnixBatch(rows: any[]) {
    if (rows.length === 0) return;

    // 1. DEDUPLICATE IN MEMORY
    const uniqueRowsMap = new Map<number, any>();
    const internalDuplicates: any[] = [];

    for (const row of rows) {
      if (!row.ticketId) continue; 

      const uniqueKey = row.ticketId;

      if (uniqueRowsMap.has(uniqueKey)) {
        internalDuplicates.push({
          ticketId: row.ticketId,
          reason: 'Duplicate found inside the same Excel batch'
        });
      }
      uniqueRowsMap.set(uniqueKey, row);
    }

    if (internalDuplicates.length > 0) {
      console.log('Internal Omnix Duplicates Skipped:', internalDuplicates.length);
    }

    const cleanRows = Array.from(uniqueRowsMap.values());
    if (cleanRows.length === 0) return;

    // 2. Map rows to SQL tuple strings
    const values = cleanRows.map((row) => {
      return `(
        ${this.formatSqlValue(row.ticketId)},
        ${this.formatSqlValue(row.remark)},
        ${this.formatSqlValue(row.subject)},
        ${this.formatSqlValue(row.priorityId)},
        ${this.formatSqlValue(row.priorityName)},
        ${this.formatSqlValue(row.ticketStatusId)},
        ${this.formatSqlValue(row.ticketStatusName)},
        ${this.formatSqlValue(row.unitId)},
        ${this.formatSqlValue(row.unitName)},
        ${this.formatSqlValue(row.informantId)},
        ${this.formatSqlValue(row.informantName)},
        ${this.formatSqlValue(row.informantHp)},
        ${this.formatSqlValue(row.informantEmail)},
        ${this.formatSqlValue(row.customerId)},
        ${this.formatSqlValue(row.customerName)},
        ${this.formatSqlValue(row.customerHp)},
        ${this.formatSqlValue(row.customerEmail)},
        ${this.formatSqlValue(row.dateOriginInteraction)},
        ${this.formatSqlValue(row.dateStartInteraction)},
        ${this.formatSqlValue(row.dateOpen)},
        ${this.formatSqlValue(row.dateClose)},
        ${this.formatSqlValue(row.dateLastUpdate)},
        ${this.formatSqlValue(row.isEscalated)},
        ${this.formatSqlValue(row.createdById)},
        ${this.formatSqlValue(row.createdByName)},
        ${this.formatSqlValue(row.updatedById)},
        ${this.formatSqlValue(row.updatedByName)},
        ${this.formatSqlValue(row.channelId)},
        ${this.formatSqlValue(row.sessionId)},
        ${this.formatSqlValue(row.categoryId)},
        ${this.formatSqlValue(row.categoryName)},
        ${this.formatSqlValue(row.dateCreatedAt)},
        ${this.formatSqlValue(row.sla)},
        ${this.formatSqlValue(row.channelName)},
        ${this.formatSqlValue(row.mainCategory)},
        ${this.formatSqlValue(row.category)},
        ${this.formatSqlValue(row.subCategory)},
        ${this.formatSqlValue(row.detailSubCategory)},
        ${this.formatSqlValue(row.detailSubCategory2)},
        ${this.formatSqlValue(row.datePickupInteraction)},
        ${this.formatSqlValue(row.dateEndInteraction)},
        ${this.formatSqlValue(row.dateFirstPickupInteraction)},
        ${this.formatSqlValue(row.dateFirstResponseInteraction)},
        ${this.formatSqlValue(row.account)},
        ${this.formatSqlValue(row.accountName)},
        ${this.formatSqlValue(row.informantMemberId)},
        ${this.formatSqlValue(row.customerMemberId)},
        ${this.formatSqlValue(row.sentimentIncoming)},
        ${this.formatSqlValue(row.sentimentOutgoing)},
        ${this.formatSqlValue(row.sentimentAll)},
        ${this.formatSqlValue(row.feedback)},
        ${this.formatSqlValue(row.sentimentService)},
        ${this.formatSqlValue(row.parentId)},
        ${this.formatSqlValue(row.countMerged)},
        ${this.formatSqlValue(row.sourceId)},
        ${this.formatSqlValue(row.sourceName)},
        ${this.formatSqlValue(row.contact)},  
        ${this.formatSqlValue(row.surveyName)},
        ${this.formatSqlValue(row.interactionAdditionalInfo)},
        ${this.formatSqlValue(row.surveyId)},
        ${this.formatSqlValue(row.respondentId)},
        ${this.formatSqlValue(row.ticketIdOld)},
        ${this.formatSqlValue(row.waitingTime)},
        ${this.formatSqlValue(row.serviceTime)},
        ${this.formatSqlValue(row.responseTime)},
        ${this.formatSqlValue(row.handlingTime)},
        ${this.formatSqlValue(row.duration)},
        ${this.formatSqlValue(row.acw)},
        ${this.formatSqlValue(row.ticketPerusahaan)},
        ${this.formatSqlValue(row.ticketAmount)},
        ${this.formatSqlValue(row.ticketRemedyNo)},
        ${this.formatSqlValue(row.ticketITAO)},
        ${this.formatSqlValue(row.ticketProject)},
        ${this.formatSqlValue(row.slaSecond)},
        ${this.formatSqlValue(row.ticketIdMasking)},
        ${this.formatSqlValue(row.informantNamaCorp)},
        ${this.formatSqlValue(row.customerNamaCorp)},
        ${this.formatSqlValue(row.datePending)},
        ${this.formatSqlValue(row.dateResolve)},
        ${this.formatSqlValue(row.dateEskalasiEbo)},
        ${this.formatSqlValue(row.dateEskalasiIt)},
        ${this.formatSqlValue(row.dateEskalasiNo)},
        ${this.formatSqlValue(row.dateEskalasi)},
        ${this.formatSqlValue(row.partner)},
        ${this.formatSqlValue(row.dateMenunggu)},
        ${this.formatSqlValue(row.approvalBillco)},
        ${this.formatSqlValue(row.customerInstagramId)},
        ${this.formatSqlValue(row.customerPhone)},
        ${this.formatSqlValue(row.customerFacebookId)}
      )`;
    }).join(',');

    const query = `
      INSERT INTO "RawOmnix" (
        "ticket_id", "remark", "subject", "priority_id", "priority_name",
        "ticket_status_id", "ticket_status_name", "unit_id", "unit_name",
        "informant_id", "informant_name", "informant_hp", "informant_email",
        "customer_id", "customer_name", "customer_hp", "customer_email",
        "date_origin_interaction", "date_start_interaction", "date_open",
        "date_close", "date_last_update", "is_escalated",
        "created_by_id", "created_by_name", "updated_by_id", "updated_by_name",
        "channel_id", "session_id", "category_id", "category_name",
        "date_created_at", "sla", "channel_name",
        "mainCategory", "category", "subCategory", "detailSubCategory", "detailSubCategory2",
        "date_pickup_interaction", "date_end_interaction", 
        "date_first_pickup_interaction", "date_first_response_interaction",
        "account", "account_name", "informant_member_id", "customer_member_id",
        "sentiment_incoming", "sentiment_outgoing", "sentiment_all", "feedback", "sentiment_service",
        "parent_id", "count_merged", "source_id", "source_name",
        "contact", "survey_name", "interaction_additional_info",
        "survey_id", "respondent_id", "ticket_id_old",
        "waitingTime", "serviceTime", "responseTime", "handlingTime", "duration", "acw",
        "ticket_perusahaan", "ticket_Amount", "ticket_Remedy_NO",
        "ticket_IT/AO", "ticket_Project", "sla_second", "ticketId_masking",
        "informant_nama_corp", "customer_nama_corp",
        "date_pending", "date_resolve", 
        "date_eskalasi ebo", "date_eskalasi it", "date_eskalasi no", "date_eskalasi",
        "partner", "date_menunggu", "approval billco",
        "customer_instagram_id", "customer_phone", "customer_facebook_id"
      )
      VALUES ${values}
      ON CONFLICT ("ticket_id") 
      DO UPDATE SET 
        "remark" = EXCLUDED."remark",
        "subject" = EXCLUDED."subject",
        "priority_id" = EXCLUDED."priority_id",
        "priority_name" = EXCLUDED."priority_name",
        "ticket_status_id" = EXCLUDED."ticket_status_id",
        "ticket_status_name" = EXCLUDED."ticket_status_name",
        "date_last_update" = EXCLUDED."date_last_update",
        "updated_by_id" = EXCLUDED."updated_by_id",
        "updated_by_name" = EXCLUDED."updated_by_name",
        "date_close" = EXCLUDED."date_close",
        "feedback" = EXCLUDED."feedback",
        "sentiment_all" = EXCLUDED."sentiment_all",
        "date_resolve" = EXCLUDED."date_resolve",
        "date_pending" = EXCLUDED."date_pending";
    `;

    await this.prisma.$executeRawUnsafe(query);
  }

  async processCsatFile(job: Job<any, any, string>): Promise<any> {
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
    
    // Check numbers specifically (NaN checks)
    if (typeof value === 'number') {
      return isNaN(value) ? 'NULL' : value.toString();
    }
    
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    
    // --- DATE HANDLING FIXED ---
    if (value instanceof Date) {
      if (isNaN(value.getTime())) {
        return 'NULL'; // Prevent "Invalid time value" crash
      }
      return `'${value.toISOString()}'`;
    }

    // Handle Objects (like JSON)
    if (typeof value === 'object') {
      const jsonString = JSON.stringify(value);
      const safeJson = jsonString.replace(/'/g, "''"); 
      return `'${safeJson}'`;
    }

    // Strings
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