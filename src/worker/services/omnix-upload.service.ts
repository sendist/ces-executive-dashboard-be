import { Injectable } from "@nestjs/common";
import { Job } from "bullmq";
import { PrismaService } from "prisma/prisma.service";
import * as ExcelJS from 'exceljs';
import { ExcelUtils } from "../excel-utils.helper";
import * as fs from 'fs';
import { calculateFcrStatus, calculateSlaStatus, determineEskalasi, TICKET_RULES, TICKET_RULES_OMNIX } from "../utils/rules.constant";


@Injectable()
export class OmnixUploadService {
  constructor(
      private readonly prisma: PrismaService,
  ){}

  // regex to identify VIP keywords
  private readonly vipRegex = /vvip|vip|direk|director|komisaris/i;

  async process(job: Job<any, any, string>): Promise<any> {

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

    // 1. Stream the Excel file
    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});
    
    
    for await (const worksheet of workbook) {
      for await (const row of worksheet) {
        if (row.number === 1) continue; // Skip Header

          const classification = this.classifyTicket(row);
    
          const rawCategory = row.getCell(36).text;
          const normalizedSubCategory =
            typeof rawCategory === 'string'
              ? rawCategory.trim().toLowerCase()
              : '';
          const derivedProduct = kipMap.get(normalizedSubCategory || '');
    
          const rawNamaPerusahaan = row.getCell(69).text;
          const normalizedNamaPerusahaan =
            typeof rawNamaPerusahaan === 'string'
              ? rawNamaPerusahaan.trim().toLowerCase()
              : '';
          const derivedAccountCategory = accountMap.get(normalizedNamaPerusahaan || '');
    
          const ticketSubject = row.getCell(3).text || '';
          const isVip = this.vipRegex.test(ticketSubject);
          
          // --- 2. RUN SLA CALCULATION ---
          // Now we pass the 'derivedProduct' as 'Kolom BF'
          const slaStatus = classification.isValid ? calculateSlaStatus({
              'product': derivedProduct, 
              'ticketCreated': row.getCell(19).text,
              'resolveTime': row.getCell(21).text
          }) : false;
    
          const fcrStatus = calculateFcrStatus({
            'ID Remedy_NO': row.getCell(71).text,
            'Eskalasi/ID Remedy_IT/AO/EMS': row.getCell(72).text,
            'Jumlah MSISDN': row['jumlah_msisdn']
          });
    
          const typeEskalasi = determineEskalasi({
            'ID Remedy_NO': row.getCell(71).text, 
            'Eskalasi/ID Remedy_IT/AO/EMS': row.getCell(72).text
          })

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
          dateOriginInteraction:      ExcelUtils.parseExcelDate(row.getCell(18).value),
          dateStartInteraction:       ExcelUtils.parseExcelDate(row.getCell(19).value),
          dateOpen:                   ExcelUtils.parseExcelDate(row.getCell(20).value),
          dateClose:                  ExcelUtils.parseExcelDate(row.getCell(21).value),
          dateLastUpdate:             ExcelUtils.parseExcelDate(row.getCell(22).value),

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
          dateCreatedAt: ExcelUtils.parseExcelDate(row.getCell(32).value),

          // --- 5. Details ---
          sla:                row.getCell(33).text,
          channelName:        row.getCell(34).text,
          mainCategory:       row.getCell(35).text,
          category:           row.getCell(36).text,
          subCategory:        row.getCell(37).text,
          detailSubCategory:  row.getCell(38).text,
          detailSubCategory2: row.getCell(39).text,

          // --- 6. More Dates ---
          datePickupInteraction:        ExcelUtils.parseExcelDate(row.getCell(40).value),
          dateEndInteraction:           ExcelUtils.parseExcelDate(row.getCell(41).value),
          dateFirstPickupInteraction:   ExcelUtils.parseExcelDate(row.getCell(42).value),
          dateFirstResponseInteraction: ExcelUtils.parseExcelDate(row.getCell(43).value),

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
          datePending:     ExcelUtils.parseExcelDate(row.getCell(78).value),
          dateResolve:     ExcelUtils.parseExcelDate(row.getCell(79).value),
          dateEskalasiEbo: ExcelUtils.parseExcelDate(row.getCell(80).value),
          dateEskalasiIt:  ExcelUtils.parseExcelDate(row.getCell(81).value),
          dateEskalasiNo:  ExcelUtils.parseExcelDate(row.getCell(82).value),
          dateEskalasi:    ExcelUtils.parseExcelDate(row.getCell(83).value),

          // --- 14. Final Fields ---
          partner:             row.getCell(84).text,
          dateMenunggu:        ExcelUtils.parseExcelDate(row.getCell(85).value),
          approvalBillco:      row.getCell(86).text,
          customerInstagramId: row.getCell(87).text,
          customerPhone:       row.getCell(88).text,
          customerFacebookId:  row.getCell(89).text,
          
          // row tambahan
          validationStatus: classification.status,
          statusTiket:      classification.isValid,
          product:          derivedProduct,
          inSla:            slaStatus,
          isFcr:            fcrStatus,
          eskalasi:         typeEskalasi,
          isPareto:         derivedAccountCategory === 'P1' ? true : false,
          isVip:            isVip,
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
    // await this.refreshDailyStats();
    
    return { status: 'Completed' };

  }

  private async saveBatch(rows: any[]) {
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
        ${ExcelUtils.formatSqlValue(row.ticketId)},
        ${ExcelUtils.formatSqlValue(row.remark)},
        ${ExcelUtils.formatSqlValue(row.subject)},
        ${ExcelUtils.formatSqlValue(row.priorityId)},
        ${ExcelUtils.formatSqlValue(row.priorityName)},
        ${ExcelUtils.formatSqlValue(row.ticketStatusId)},
        ${ExcelUtils.formatSqlValue(row.ticketStatusName)},
        ${ExcelUtils.formatSqlValue(row.unitId)},
        ${ExcelUtils.formatSqlValue(row.unitName)},
        ${ExcelUtils.formatSqlValue(row.informantId)},
        ${ExcelUtils.formatSqlValue(row.informantName)},
        ${ExcelUtils.formatSqlValue(row.informantHp)},
        ${ExcelUtils.formatSqlValue(row.informantEmail)},
        ${ExcelUtils.formatSqlValue(row.customerId)},
        ${ExcelUtils.formatSqlValue(row.customerName)},
        ${ExcelUtils.formatSqlValue(row.customerHp)},
        ${ExcelUtils.formatSqlValue(row.customerEmail)},
        ${ExcelUtils.formatSqlValue(row.dateOriginInteraction)},
        ${ExcelUtils.formatSqlValue(row.dateStartInteraction)},
        ${ExcelUtils.formatSqlValue(row.dateOpen)},
        ${ExcelUtils.formatSqlValue(row.dateClose)},
        ${ExcelUtils.formatSqlValue(row.dateLastUpdate)},
        ${ExcelUtils.formatSqlValue(row.isEscalated)},
        ${ExcelUtils.formatSqlValue(row.createdById)},
        ${ExcelUtils.formatSqlValue(row.createdByName)},
        ${ExcelUtils.formatSqlValue(row.updatedById)},
        ${ExcelUtils.formatSqlValue(row.updatedByName)},
        ${ExcelUtils.formatSqlValue(row.channelId)},
        ${ExcelUtils.formatSqlValue(row.sessionId)},
        ${ExcelUtils.formatSqlValue(row.categoryId)},
        ${ExcelUtils.formatSqlValue(row.categoryName)},
        ${ExcelUtils.formatSqlValue(row.dateCreatedAt)},
        ${ExcelUtils.formatSqlValue(row.sla)},
        ${ExcelUtils.formatSqlValue(row.channelName)},
        ${ExcelUtils.formatSqlValue(row.mainCategory)},
        ${ExcelUtils.formatSqlValue(row.category)},
        ${ExcelUtils.formatSqlValue(row.subCategory)},
        ${ExcelUtils.formatSqlValue(row.detailSubCategory)},
        ${ExcelUtils.formatSqlValue(row.detailSubCategory2)},
        ${ExcelUtils.formatSqlValue(row.datePickupInteraction)},
        ${ExcelUtils.formatSqlValue(row.dateEndInteraction)},
        ${ExcelUtils.formatSqlValue(row.dateFirstPickupInteraction)},
        ${ExcelUtils.formatSqlValue(row.dateFirstResponseInteraction)},
        ${ExcelUtils.formatSqlValue(row.account)},
        ${ExcelUtils.formatSqlValue(row.accountName)},
        ${ExcelUtils.formatSqlValue(row.informantMemberId)},
        ${ExcelUtils.formatSqlValue(row.customerMemberId)},
        ${ExcelUtils.formatSqlValue(row.sentimentIncoming)},
        ${ExcelUtils.formatSqlValue(row.sentimentOutgoing)},
        ${ExcelUtils.formatSqlValue(row.sentimentAll)},
        ${ExcelUtils.formatSqlValue(row.feedback)},
        ${ExcelUtils.formatSqlValue(row.sentimentService)},
        ${ExcelUtils.formatSqlValue(row.parentId)},
        ${ExcelUtils.formatSqlValue(row.countMerged)},
        ${ExcelUtils.formatSqlValue(row.sourceId)},
        ${ExcelUtils.formatSqlValue(row.sourceName)},
        ${ExcelUtils.formatSqlValue(row.contact)},  
        ${ExcelUtils.formatSqlValue(row.surveyName)},
        ${ExcelUtils.formatSqlValue(row.interactionAdditionalInfo)},
        ${ExcelUtils.formatSqlValue(row.surveyId)},
        ${ExcelUtils.formatSqlValue(row.respondentId)},
        ${ExcelUtils.formatSqlValue(row.ticketIdOld)},
        ${ExcelUtils.formatSqlValue(row.waitingTime)},
        ${ExcelUtils.formatSqlValue(row.serviceTime)},
        ${ExcelUtils.formatSqlValue(row.responseTime)},
        ${ExcelUtils.formatSqlValue(row.handlingTime)},
        ${ExcelUtils.formatSqlValue(row.duration)},
        ${ExcelUtils.formatSqlValue(row.acw)},
        ${ExcelUtils.formatSqlValue(row.ticketPerusahaan)},
        ${ExcelUtils.formatSqlValue(row.ticketAmount)},
        ${ExcelUtils.formatSqlValue(row.ticketRemedyNo)},
        ${ExcelUtils.formatSqlValue(row.ticketITAO)},
        ${ExcelUtils.formatSqlValue(row.ticketProject)},
        ${ExcelUtils.formatSqlValue(row.slaSecond)},
        ${ExcelUtils.formatSqlValue(row.ticketIdMasking)},
        ${ExcelUtils.formatSqlValue(row.informantNamaCorp)},
        ${ExcelUtils.formatSqlValue(row.customerNamaCorp)},
        ${ExcelUtils.formatSqlValue(row.datePending)},
        ${ExcelUtils.formatSqlValue(row.dateResolve)},
        ${ExcelUtils.formatSqlValue(row.dateEskalasiEbo)},
        ${ExcelUtils.formatSqlValue(row.dateEskalasiIt)},
        ${ExcelUtils.formatSqlValue(row.dateEskalasiNo)},
        ${ExcelUtils.formatSqlValue(row.dateEskalasi)},
        ${ExcelUtils.formatSqlValue(row.partner)},
        ${ExcelUtils.formatSqlValue(row.dateMenunggu)},
        ${ExcelUtils.formatSqlValue(row.approvalBillco)},
        ${ExcelUtils.formatSqlValue(row.customerInstagramId)},
        ${ExcelUtils.formatSqlValue(row.customerPhone)},
        ${ExcelUtils.formatSqlValue(row.customerFacebookId)},
        ${ExcelUtils.formatSqlValue(row.validationStatus)},
        ${ExcelUtils.formatSqlValue(row.statusTiket)},
        ${ExcelUtils.formatSqlValue(row.product)},
        ${ExcelUtils.formatSqlValue(row.inSla)},
        ${ExcelUtils.formatSqlValue(row.isFcr)},
        ${ExcelUtils.formatSqlValue(row.eskalasi)},
        ${ExcelUtils.formatSqlValue(row.isVip)},
        ${ExcelUtils.formatSqlValue(row.isPareto)}
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
        "customer_instagram_id", "customer_phone", "customer_facebook_id",
        "validationStatus", "statusTiket", "product","inSla", "isFcr", "eskalasi", "isVip", "isPareto"
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

  private classifyTicket(row: any) {
    // 1. Iterate through defined rules
    for (const rule of TICKET_RULES_OMNIX) {
        // Get value safely (handle casing if needed)
        // const cellValue = row[rule.column]; 
        const cellValue = row.getCell(rule.column).text;
        
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
    // if (row['description'] && /completed by hia/i.test(row['description'])) {
    //      return { status: 'Valid', isValid: true, reason: 'Completed by HIA' };
    // }

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
}