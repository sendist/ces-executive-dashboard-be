import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from 'prisma/prisma.service';
import { ExcelUtils } from '../excel-utils.helper';
import {
  calculateFcrStatus,
  calculateSlaStatus,
  determineEskalasi,
  TICKET_RULES,
} from '../utils/rules.constant';

@Injectable()
export class OcaUpsertService {
  private readonly logger = new Logger(OcaUpsertService.name);
  constructor(private readonly prisma: PrismaService) {}

  async saveBatch(rows: any[]) {
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
    const values = rows
      .map((row) => {
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
      })
      .join(',');

    // 3. EXECUTE QUERY with SNAKE_CASE columns
    const query = `
    WITH upsert AS(
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
    "ticket_subject"              = EXCLUDED."ticket_subject",
    "channel"                     = EXCLUDED."channel",
    "category"                    = EXCLUDED."category",
    "reporter"                    = EXCLUDED."reporter",
    "assignee"                    = EXCLUDED."assignee",
    "department"                  = EXCLUDED."department",
    "priority"                     = EXCLUDED."priority",
    "last_status"                 = EXCLUDED."last_status",
    "ticket_created"              = EXCLUDED."ticket_created",
    "last_update"                 = EXCLUDED."last_update",
    "description"                 = EXCLUDED."description",
    "customer_name"               = EXCLUDED."customer_name",
    "customer_phone"              = EXCLUDED."customer_phone",
    "customer_address"            = EXCLUDED."customer_address",
    "customer_email"              = EXCLUDED."customer_email",
    "first_response_time"         = EXCLUDED."first_response_time",
    "total_response_time"         = EXCLUDED."total_response_time",
    "total_resolution_time"       = EXCLUDED."total_resolution_time",
    "resolve_time"                = EXCLUDED."resolve_time",
    "resolved_by"                 = EXCLUDED."resolved_by",
    "closed_time"                 = EXCLUDED."closed_time",
    "ticket_duration"             = EXCLUDED."ticket_duration",
    "count_inbound_message"       = EXCLUDED."count_inbound_message",
    "label_in_room"               = EXCLUDED."label_in_room",
    "first_response_duration"     = EXCLUDED."first_response_duration",
    "escalate_ticket"             = EXCLUDED."escalate_ticket",
    "last_assignee_escalation"    = EXCLUDED."last_assignee_escalation",
    "last_status_escalation"      = EXCLUDED."last_status_escalation",
    "last_update_escalation"      = EXCLUDED."last_update_escalation",
    "converse"                    = EXCLUDED."converse",
    "move_to_other_channel"       = EXCLUDED."move_to_other_channel",
    "previous_channel"            = EXCLUDED."previous_channel",
    "amount_revenue"              = EXCLUDED."amount_revenue",
    "jumlah_msisdn"               = EXCLUDED."jumlah_msisdn",
    "tags"                        = EXCLUDED."tags",
    "id_remedy_no"                = EXCLUDED."id_remedy_no",
    "eskalasi_id_remedy_it_ao_ems" = EXCLUDED."eskalasi_id_remedy_it_ao_ems",
    "reason_osl"                  = EXCLUDED."reason_osl",
    "project_id"                  = EXCLUDED."project_id",
    "nama_perusahaan"             = EXCLUDED."nama_perusahaan",
    "roaming"                     = EXCLUDED."roaming",
    "sub_category"                = EXCLUDED."sub_category",
    "detail_category"             = EXCLUDED."detail_category",
    "iot"                         = EXCLUDED."iot",
    "validationStatus"            = EXCLUDED."validationStatus",
    "statusTiket"                 = EXCLUDED."statusTiket",
    "product"                     = EXCLUDED."product",
    "inSla"                       = EXCLUDED."inSla",
    "isFcr"                       = EXCLUDED."isFcr",
    "eskalasi"                    = EXCLUDED."eskalasi",
    "isVip"                       = EXCLUDED."isVip",
    "isPareto"                    = EXCLUDED."isPareto",
    "updated_at_excel"            = EXCLUDED."updated_at_excel"
    WHERE "RawOca"."last_update" IS DISTINCT FROM EXCLUDED."last_update"
    RETURNING xmax
    )
    SELECT
      COUNT(*) FILTER (WHERE xmax = 0)::int AS inserted,
      COUNT(*) FILTER (WHERE xmax <> 0)::int AS updated
      FROM upsert;
    `;

    const result =
      await this.prisma.$queryRawUnsafe<
        { inserted: number; updated: number }[]
      >(query);

    const { inserted, updated } = result[0];

    this.logger.log(
      `Oca Upsert completed: ${inserted} inserted, ${updated} updated`,
    );
  }
}
