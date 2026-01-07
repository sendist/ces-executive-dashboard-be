// const ExcelJS = require('exceljs');

// // --- THE FIX: Your Helper Function ---
// function parseExcelDate(value) {
//     if (!value) return null;

//     // CASE 1: Value is a Number (Excel Serial Date)
//     // Example: 45658.25 is Jan 1, 2025
//     if (typeof value === 'number') {
//         // Excel epoch (Dec 30 1899) -> Unix epoch (Jan 1 1970) = 25569 days
//         // 86400000 = ms per day
//         const date = new Date((value - 25569) * 86400000);
//         // Adjust for timezone offset if necessary, but usually UTC is safer
//         return date;
//     }

//     // CASE 2: Value is already a Date object
//     if (value instanceof Date) {
//         return value;
//     }

//     // CASE 3: Value is a String ("01/01/2025 06:06:10")
//     if (typeof value === 'string') {
//         // Split "01/01/2025 06:06:10"
//         // Adjust split logic based on your exact format
//         const [datePart, timePart] = value.split(' ');
//         if (!datePart) return null;

//         const [day, month, year] = datePart.split('/');
//         const [hour, minute, second] = timePart ? timePart.split(':') : ['00', '00', '00'];

//         // Note: Month is 0-indexed in JS (0=Jan)
//         return new Date(
//             parseInt(year),
//             parseInt(month) - 1,
//             parseInt(day),
//             parseInt(hour || 0),
//             parseInt(minute || 0),
//             parseInt(second || 0)
//         );
//     }

//     // CASE 4: Hyperlinks/Formulas (ExcelJS returns objects sometimes)
//     if (typeof value === 'object' && value.result) {
//         return parseExcelDate(value.result);
//     }

//     return null;
// }

// // --- MAIN TEST RUNNER ---
// async function runTest() {
//     console.log("--- 1. Creating Mock Excel File ---");
//     const workbook = new ExcelJS.Workbook();
//     const sheet = workbook.addWorksheet('Test');

//     // Add headers
//     sheet.addRow(['ID', 'Created At (Mixed Formats)']);

//     // Row 2: Text String Format (What you see in CSVs usually)
//     sheet.addRow([1, "01/01/2025 06:06:10"]);

//     // Row 3: Excel Serial Number (What you often get from .xlsx)
//     // 45658.25428 = Jan 01 2025 ~06:06
//     sheet.addRow([2, 45658.2542824074]);

//     // Row 4: Actual Date Object (If ExcelJS parses it automatically)
//     sheet.addRow([3, new Date('2025-01-01T06:06:10')]);

//     await workbook.xlsx.writeFile('temp_test.xlsx');
//     console.log("File created: temp_test.xlsx\n");

//     console.log("--- 2. Reading & Converting ---");
    
//     // Mimic your processor logic
//     const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader('temp_test.xlsx', {});
    
//     for await (const worksheet of workbookReader) {
//         for await (const row of worksheet) {
//             if (row.number === 1) continue; // Skip header

//             const rawValue = row.getCell(2).value;
//             const parsedDate = parseExcelDate(rawValue);

//             console.log(`\nRow ${row.number}:`);
//             console.log(`Type:       ${typeof rawValue}`);
//             console.log(`Raw Value:  ${JSON.stringify(rawValue)}`);
//             console.log(`Parsed Date: ${parsedDate ? parsedDate.toString() : 'Invalid'}`);
//             console.log(`ISO String: ${parsedDate ? parsedDate.toISOString() : 'Invalid'}`);
//         }
//     }
// }

// runTest();


function extractFirstId(rawValue) {
  if (!rawValue) return null;

  // 1. Ensure it's a string
  const strValue = rawValue.toString();

  // 2. Split by semicolon first (to handle multiple IDs)
  const firstPart = strValue.split(';')[0];

  // 3. Remove quotes (both " and ') AND trim whitespace
  //    Regex /['"]/g matches all instances of ' or "
  const cleanId = firstPart.replace(/['"]/g, '').trim();

  // 4. Basic safety
  return cleanId.length > 0 ? cleanId : null;
}

console.log(extractFirstId("6771f246a7962d0011b0ef92; 6771fad1f4bfcd00123fbd2d; 6772958b5a78540011416d0a; 677368ae55555c0012965c21"));
console.log(extractFirstId('"6771fac15a785400113cd675"'));



     INSERT INTO "DailyCsatStat" (
        "date", 
        "totalSurvey", 
        "totalDijawab", 
        "totalJawaban45", 
        "scoreCsat", 
        "persenCsat"
      )
      SELECT 
        DATE("createdAt") as date,
        COUNT(*) as totalSurvey,
        
        -- Count only if answeredAt is present
        COUNT(CASE WHEN "answeredAt" IS NOT NULL THEN 1 END) as totalDijawab,
        
        -- Count only if Score is 4 or 5
        COUNT(CASE WHEN "numeric" >= 4 THEN 1 END) as totalJawaban45,
        
        -- CSAT Score
        -- persenCsat * 5 as scoreCsat,
        COALESCE(AVG("numeric"), 0) as scoreCsat,

        -- Calculate Percentage: (Total 4-5 / Total Answered) * 100
        CASE 
          WHEN COUNT(CASE WHEN "answeredAt" IS NOT NULL THEN 1 END) = 0 THEN 0
          ELSE (CAST(COUNT(CASE WHEN "numeric" >= 4 THEN 1 END) AS FLOAT) / 
                CAST(COUNT(CASE WHEN "answeredAt" IS NOT NULL THEN 1 END) AS FLOAT)) * 100
        END as persenCsat
        

      FROM "RawCsat"
      GROUP BY DATE("createdAt")
      ON CONFLICT ("date") 
      DO UPDATE SET 
        "totalSurvey" = EXCLUDED."totalSurvey",
        "totalDijawab" = EXCLUDED."totalDijawab",
        "totalJawaban45" = EXCLUDED."totalJawaban45",
        "scoreCsat" = EXCLUDED."scoreCsat",
        "persenCsat" = EXCLUDED."persenCsat";
    `;