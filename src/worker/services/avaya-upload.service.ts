import { Injectable } from "@nestjs/common";
import { Job } from "bullmq";
import { PrismaService } from "prisma/prisma.service";
import * as ExcelJS from 'exceljs';
import { ExcelUtils } from "../excel-utils.helper";
import * as fs from 'fs';

@Injectable()
export class AvayaUploadService {
    constructor(private readonly prisma: PrismaService) {}

    async process(job: Job) {
        const filePath = job.data.path;
        if (!fs.existsSync(filePath)) {
            throw new Error(`File not found: ${filePath}`);
        }

        const batchSize = 1000;
        let rowsToInsert: any[] = [];

        const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});

        for await (const worksheet of workbook) {
            for await (const row of worksheet) {
                if (row.number === 1 || row.number === 2 ) continue; // Skip Header

                // Helper to clean numeric values (handles "1.234,56" or "123" strings)
                const parseNum = (val: any) => {
                    if (val === null || val === undefined) return 0;
                    const clean = val.toString().replace(',', '.');
                    return parseFloat(clean) || 0;
                };

                const rowData = {
                    date: ExcelUtils.parseExcelDate(row.getCell(1).value),
                    vector: parseInt(row.getCell(2).text) || 0,
                    inboundCalls: parseNum(row.getCell(3).value),
                    flowIn: parseNum(row.getCell(4).value),
                    acdCalls: parseNum(row.getCell(5).value),
                    acdTime: parseNum(row.getCell(6).value),
                    holdTime: parseNum(row.getCell(7).value),
                    aht: parseNum(row.getCell(8).value),
                    avgSpeedAns: parseNum(row.getCell(9).value),
                    avgAcdTime: parseNum(row.getCell(10).value),
                    avgAcwTime: parseNum(row.getCell(11).value),
                    mainAcdCalls: parseNum(row.getCell(12).value),
                    backupAcdCalls: parseNum(row.getCell(13).value),
                    connectCalls: parseNum(row.getCell(14).value),
                    avgConnectTime: parseNum(row.getCell(15).value),
                    abanCalls: parseNum(row.getCell(16).value),
                    avgAbanTime: parseNum(row.getCell(17).value),
                    percentAban: parseNum(row.getCell(18).value),
                    forcedBusyCalls: parseNum(row.getCell(19).value),
                    percentBusy: parseNum(row.getCell(20).value),
                    forcedDiscCalls: parseNum(row.getCell(21).value),
                    flowOut: parseNum(row.getCell(22).value),
                    percentFlowOut: parseNum(row.getCell(23).value),
                    avgVdnTime: parseNum(row.getCell(24).value),
                    skillPref1: parseNum(row.getCell(25).value),
                    skillPref2: parseNum(row.getCell(26).value),
                    skillPref3: parseNum(row.getCell(27).value),
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

        return { status: 'Call Metrics Upload Completed' };
    }

    private async saveBatch(rows: any[]) {
        if (rows.length === 0) return;

        // 1. DEDUPLICATE IN MEMORY (Date + Vector)
        const uniqueRowsMap = new Map<string, any>();
        for (const row of rows) {
            const uniqueKey = `${row.date.toISOString().split('T')[0]}_${row.vector}`;
            uniqueRowsMap.set(uniqueKey, row);
        }

        const cleanRows = Array.from(uniqueRowsMap.values());

        // 2. BUILD SQL VALUES
        const values = cleanRows.map((r) => {
            return `(
                ${ExcelUtils.formatSqlValue(r.date)}, ${r.vector}, ${r.inboundCalls}, ${r.flowIn}, 
                ${r.acdCalls}, ${r.acdTime}, ${r.holdTime}, ${r.aht}, ${r.avgSpeedAns}, 
                ${r.avgAcdTime}, ${r.avgAcwTime}, ${r.mainAcdCalls}, ${r.backupAcdCalls}, 
                ${r.connectCalls}, ${r.avgConnectTime}, ${r.abanCalls}, ${r.avgAbanTime}, 
                ${r.percentAban}, ${r.forcedBusyCalls}, ${r.percentBusy}, ${r.forcedDiscCalls}, 
                ${r.flowOut}, ${r.percentFlowOut}, ${r.avgVdnTime}, ${r.skillPref1}, 
                ${r.skillPref2}, ${r.skillPref3}
            )`;
        }).join(',');

        console.log(values)

        // 3. EXECUTE UPSERT (Mapping to the snake_case names in DB)
        const query = `
            INSERT INTO "raw_avaya" (
                "date", "vector", "inbound_calls", "flow_in", "acd_calls", "acd_time", 
                "hold_time", "aht", "avg_speed_ans", "avg_acd_time", "avg_acw_time", 
                "main_acd_calls", "backup_acd_calls", "connect_calls", "avg_connect_time", 
                "aban_calls", "avg_aban_time", "percent_aban", "forced_busy_calls", 
                "percent_busy", "forced_disc_calls", "flow_out", "percent_flow_out", 
                "avg_vdn_time", "1st_skill_pref", "2nd_skill_pref", "3rd_skill_pref"
            )
            VALUES ${values}
            ON CONFLICT ("date", "vector")
            DO UPDATE SET
                "inbound_calls"      = EXCLUDED."inbound_calls",
                "flow_in"            = EXCLUDED."flow_in",
                "acd_calls"          = EXCLUDED."acd_calls",
                "acd_time"           = EXCLUDED."acd_time",
                "hold_time"          = EXCLUDED."hold_time",
                "aht"                = EXCLUDED."aht",
                "avg_speed_ans"      = EXCLUDED."avg_speed_ans",
                "avg_acd_time"       = EXCLUDED."avg_acd_time",
                "avg_acw_time"       = EXCLUDED."avg_acw_time",
                "main_acd_calls"     = EXCLUDED."main_acd_calls",
                "backup_acd_calls"   = EXCLUDED."backup_acd_calls",
                "connect_calls"      = EXCLUDED."connect_calls",
                "avg_connect_time"   = EXCLUDED."avg_connect_time",
                "aban_calls"         = EXCLUDED."aban_calls",
                "avg_aban_time"      = EXCLUDED."avg_aban_time",
                "percent_aban"       = EXCLUDED."percent_aban",
                "forced_busy_calls"  = EXCLUDED."forced_busy_calls",
                "percent_busy"       = EXCLUDED."percent_busy",
                "forced_disc_calls"  = EXCLUDED."forced_disc_calls",
                "flow_out"           = EXCLUDED."flow_out",
                "percent_flow_out"   = EXCLUDED."percent_flow_out",
                "avg_vdn_time"       = EXCLUDED."avg_vdn_time",
                "1st_skill_pref"     = EXCLUDED."1st_skill_pref",
                "2nd_skill_pref"     = EXCLUDED."2nd_skill_pref",
                "3rd_skill_pref"     = EXCLUDED."3rd_skill_pref"
            ;
        `;

        await this.prisma.$executeRawUnsafe(query);
    }
}