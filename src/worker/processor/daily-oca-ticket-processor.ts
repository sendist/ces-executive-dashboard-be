// ticket.processor.ts
import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import axios from 'axios';
import { PrismaService } from 'prisma/prisma.service';
import {
  calculateFcrStatus,
  calculateSlaStatus,
  determineEskalasi,
  TICKET_RULES,
} from '../utils/rules.constant';
import { OcaUpsertService } from '../repository/oca-upsert.service';
import { Logger } from '@nestjs/common';
import { ExcelUtils } from '../excel-utils.helper';

@Processor('ticket-processing')
export class DailyOcaTicketProcessor extends WorkerHost {
  private readonly logger = new Logger(DailyOcaTicketProcessor.name);
  constructor(
    private readonly prisma: PrismaService, // Assuming Prisma
    private readonly ocaUpsertService: OcaUpsertService,
    // Inject your logic services here
  ) {
    super();
  }
  // regex to identify VIP keywords
  private readonly vipRegex = /vvip|vip|direk|director|komisaris/i;

  async process(job: Job<any, any, string>): Promise<any> {
    const { ticketId, baseData } = job.data;
    const { tickets } = job.data; // <--- Receive Array
    const resultsToUpsert = [];

    const kipMap = await this.createLookupMap(
      this.prisma.kIP,
      'subCategory',
      'product',
    );

    const accountMap = await this.createLookupMap(
      this.prisma.accountMapping,
      'corporateName',
      'kategoriAccount',
    );

    this.logger.log(`Processing batch of ${tickets.length} tickets...`); // 1. Fetch Activity History

    // 2. Process all tickets in the batch concurrently
    // We use Promise.all to hit the API for all 20 tickets in parallel (much faster)
    const processPromises = tickets.map(async (baseTicket) => {
      try {
        // A. Hit API
        const activityRes = await axios.post(
          'https://webapigw.ocatelkom.co.id/oca-interaction/ticketing/list-activity',
          { ticket_id: baseTicket.ticket_id },
          {
            auth: {
              username: 'tsel-app-connectivity',
              password: '@tsel198xMu918230pp',
            },
          },
        );

        // B. Logic (Reconstruct & Map)
        const activities = activityRes.data.results || [];
        const customFields = this.extractLatestCustomFields(activities);

        // Pass maps into mapToDomainModel if needed, or use them here
        let mappedData = this.mapToDomainModel(baseTicket, customFields);

        // C. Calculations (Using the Maps we fetched once)
        const classification = this.classifyTicket(mappedData);

        const rawSubCategory = mappedData.subCategory;
        const normalizedSubCategory =
          typeof rawSubCategory === 'string'
            ? rawSubCategory.trim().toLowerCase()
            : '';
        const derivedProduct = kipMap.get(normalizedSubCategory || '');

        const rawNamaPerusahaan = mappedData.namaPerusahaan;
        const normalizedNamaPerusahaan =
          typeof rawNamaPerusahaan === 'string'
            ? rawNamaPerusahaan.trim().toLowerCase()
            : '';
        const derivedAccountCategory = accountMap.get(
          normalizedNamaPerusahaan || '',
        );

        const ticketSubject = mappedData.ticketSubject || '';
        const isVip = this.vipRegex.test(ticketSubject);

        // --- 2. RUN SLA CALCULATION ---
        // Now we pass the 'derivedProduct' as 'Kolom BF'
        const slaStatus = classification.isValid
          ? calculateSlaStatus({
              product: derivedProduct,
              ticketCreated: mappedData.ticketCreated,
              resolveTime: mappedData.resolveTime,
            })
          : false;

        const fcrStatus = calculateFcrStatus({
          'ID Remedy_NO': mappedData.idRemedyNo,
          'Eskalasi/ID Remedy_IT/AO/EMS': mappedData.eskalasiId,
          'Jumlah MSISDN': mappedData.jumlahMsisdn,
        });

        const typeEskalasi = determineEskalasi({
          'ID Remedy_NO': mappedData.idRemedyNo,
          'Eskalasi/ID Remedy_IT/AO/EMS': mappedData.eskalasiId,
        });

        // D. Return Final Object
        return {
          ...mappedData,
          validationStatus: classification.status,
          statusTiket: classification.isValid,
          product: derivedProduct,
          sla: slaStatus,
          fcr: fcrStatus,
          eskalasi: typeEskalasi,
          isPareto: derivedAccountCategory === 'P1' ? true : false,
          isVip: isVip,
        };
      } catch (error) {
        this.logger.error(
          `Failed to process ticket ${baseTicket.ticket_id}`,
          error,
        );
        return null; // Return null so we can filter it out later
      }
    });

    // Wait for all API calls to finish
    const processedResults = await Promise.all(processPromises);

    // Filter out any failures (nulls)
    const validRows = processedResults.filter((row) => row !== null);

    // 3. Save as BATCH (Single Database Transaction)
    if (validRows.length > 0) {
      await this.ocaUpsertService.saveBatch(validRows);
      this.logger.log(`Successfully saved ${validRows.length} tickets.`);
    }

  }

  /**
   * Helper to look through activity logs and find the last known value
   * for fields that only appear in "changes"
   */
  private extractLatestCustomFields(activities: any[]) {
    // Default values
    const state = {
      'Amount Revenue': '0',
      'ID Remedy_NO': '',
      'Jumlah MSISDN': '0',
      'Sub Category': '',
      'Nama Perusahaan': '',
      'Eskalasi/ID Remedy_IT/AO/EMS': '',
      category: '',
      Reporter: '',
      Tags: '',
      'Reason OSL': '',
      'Project ID': '',
      Roaming: '',
      'Detail Category': '',
    };

    // Sort activities oldest to newest to replay history correctly
    // (Assuming API returns newest first, so we reverse or iterate backwards)
    const sortedActivities = activities.sort(
      (a, b) =>
        new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime(),
    );

    for (const act of sortedActivities) {
      // 1. Safe extraction into a variable
      const changes = act.object?.additional_info?.changes;

      if (Array.isArray(changes)) {
        for (const change of changes) {
          // If this change is about a field we care about, update our state
          if (state.hasOwnProperty(change.name)) {
            state[change.name] = change.to;
          }
        }
      } else if (changes && typeof changes === 'object') {
        // Log it to see if we are missing data, or just ignore it
        //  console.warn('Found non-array changes:', changes);
      }
      if (act.object?.creator_info?.name) {
        state['Reporter'] = act.object.creator_info.name;
      }
    }
    return state;
  }

  private mapToDomainModel(baseData: any, customFields: any) {
    return {
      ticketNumber: baseData.ticket_number,
      ticketSubject: baseData.ticket_subject,
      channel: baseData.channel,
      category: customFields['category'],
      reporter: customFields['Reporter'],
      assignee: baseData.assigned_data?.name ?? '-',
      department: baseData.department_data?.name ?? '-',
      priority: baseData.priority,
      lastStatus: baseData.status,
      // status: baseData.status,

      ticketCreated: baseData.created_at,
      lastUpdate: baseData.updated_at,

      description: baseData.detail,
      customerName: baseData.client_name,
      customerPhone: baseData.phone_number,
      customerAddress: '-', //TODO: cari field address dari mana
      customerEmail: baseData.client_name,

      firstResponseTime: baseData.as_ticket?.first_executed_at ?? null,
      totalResponseTime: baseData.as_ticket?.resolved_at ?? '~', //TODO: hitung dari resolve - createdAt
      totalResolutionTime: baseData.as_ticket?.resolved_at ?? '-', //TODO: hitung dari resolve - createdAt
      resolveTime: baseData.as_ticket?.resolved_at ?? null,
      resolvedBy: 'agent', //TODO: kemungkinan besar agent
      closedTime: baseData.as_ticket?.resolved_at ?? null, //TODO: harus cari dari activity timestap pas closed
      ticketDuration: '-', //TODO: hitung

      countInboundMessage: 0, //TODO: cari tau dari mana
      lablInRoom: baseData.room, //TODO: baru dapat idRoom
      firstResponseDuration: '-', //TODO: hitung

      escalateTicket: baseData.escalation_to,
      lastAssigneeEscalation: '-',
      lastStatusEscalation: '-',
      lastUpdateEscalation: '-',

      converse: baseData.converse,
      moveToOtherChannel: 'No',
      previousChannel: '-',

      // amountRevenue: BigInt(customFields['Amount Revenue'] || 0),
      amountRevenue: ExcelUtils.parseSafeBigInt(
        customFields['Amount Revenue'] || 0,
      ),
      jumlahMsisdn: customFields['Jumlah MSISDN'],

      tags: customFields['Tags'],
      idRemedyNo: customFields['ID Remedy_NO'],
      eskalasiId: customFields['Eskalasi/ID Remedy_IT/AO/EMS'],
      reasonOsl: customFields['Reason OSL'],
      projectId: customFields['Project ID'],
      namaPerusahaan: customFields['Nama Perusahaan'],
      roaming: customFields['Roaming'],
      subCategory: customFields['Sub Category'],
      detailCategory: customFields['Detail Category'],
      iot: customFields['IOT'],
    };
  }

  // ... Include your calculateSlaStatus, determineEskalasi, etc methods here
  private classifyTicket(row: any) {
    // 1. Iterate through defined rules
    for (const rule of TICKET_RULES) {
      // Get value safely (handle casing if needed)
      const cellValue = row[rule.prop];

      // If rule matches, return that status immediately (Fail-Fast)
      if (cellValue && rule.check(cellValue)) {
        return {
          status: rule.status,
          isValid: false, // It hit a "Double/EMS/RPA" rule
          reason: `Matched ${rule.status} rule on ${rule.column}`,
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
