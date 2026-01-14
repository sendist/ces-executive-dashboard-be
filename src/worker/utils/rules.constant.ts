import { parse, differenceInMilliseconds, isValid } from 'date-fns';
import { ExcelUtils } from '../excel-utils.helper';


// 1. Helper to turn "spam / out of topic / ..." into a fast Regex
const createRegex = (slashSeparatedString: string) => {
  // Escape special chars, split by '/', trim whitespace, join with OR pipe
  const pattern = slashSeparatedString
    .split('/')
    .map((s) => s.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('|');
  return new RegExp(pattern, 'i'); // 'i' = case insensitive
};

export const TICKET_RULES = [
  // --- PRIORITY 1: Specific Email/Subject Checks (Fastest) ---
  {
    status: 'EMS',
    column: 'customerEmail',
    check: (val: string) => val === 'ems@telkomsel.co.id', // Exact match
  },
  {
    status: 'RPA',
    column: 'customerEmail',
    check: (val: string) => val === 'rpa_ces@telkomsel.co.id',
  },
  {
    status: 'HIA',
    column: 'ticketSubject',
    check: (val: string) => val === 'UAT HIA',
  },

  // --- PRIORITY 2: Double / Spam Checks (Complex Text) ---
  {
    status: 'Double',
    column: 'department',
    check: (val: string) => val === 'Tiket Take Out',
  },
  {
    status: 'Double',
    column: 'channel',
    check: (val: string) => val === 'Live Chat',
  },
  {
    status: 'Double',
    column: 'assignee',
    check: (val: string) => val === 'TL Iwan Hermawan',
  },
  {
    status: 'Double',
    column: 'description',
    // Pre-compiled Regex for speed
    regex: createRegex('spam / out of topic / double ticket / dobel ticket / double tiket / dobel tiket / balikan ems / balasan ems'),
    check: function(val: string) { return this.regex.test(val || ''); }
  },
  {
    status: 'Double',
    column: 'detailCategory',
    regex: createRegex('I12-Status ticket / I12-Ticket ID / I11-Interaksi terputus / I12-Out Of Topic / Out Of Topic'),
    check: function(val: string) { return this.regex.test(val || ''); }
  },
  
  // --- PRIORITY 3: RPA Description Check ---
  {
    status: 'RPA',
    column: 'description',
    check: (val: string) => (val || '').trim() === 'RPA',
  },
];

export const TICKET_RULES_OMNIX = [
  {
    status: 'Double',
    column: 51,
    // Pre-compiled Regex for speed
    regex: createRegex('spam / out of topic / double ticket / dobel ticket / double tiket / dobel tiket / balikan ems / balasan ems'),
    check: function(val: string) { return this.regex.test(val || ''); }
  },
  {
    status: 'Double',
    column: 37,
    regex: createRegex('Out Of Topic / Interaksi terputus / Pelanggan iseng'),
    check: function(val: string) { return this.regex.test(val || ''); }
  },
];


// Config constants for readability (and easy changing later)
const SLA_LIMITS = {
  CONNECTIVITY: 3 * 60 * 60 * 1000, // 3 Hours in ms
  SOLUTION: 6 * 60 * 60 * 1000,     // 6 Hours in ms
};

export function calculateSlaStatus(row: any): boolean {
  // 1. Extract raw values
  const type = row['product'] || ''; 
  const createdRaw = row['ticketCreated']; 
  const resolutionRaw = row['resolveTime'];

  // 2. Handle "Blank" or "-" logic
  // Check the raw value first
  if (!resolutionRaw || resolutionRaw === '-') {
    return true; // Implies IN SLA
  }

  // 3. Parse Dates using your robust ExcelUtils
  // converting whatever mess (Number/String/Date) into a valid JS Date
  const createdDate = ExcelUtils.parseExcelDate(createdRaw);
  const resolutionDate = ExcelUtils.parseExcelDate(resolutionRaw);

  // Safety check: Ensure we actually got valid dates back
  if (!createdDate || !resolutionDate || !isValid(createdDate) || !isValid(resolutionDate)) {
    console.warn(`SLA Calculation Failed: Invalid Date. Created: ${createdRaw}, Res: ${resolutionRaw}`);
    return false; 
  }

  // 4. Calculate Duration (Now safe because inputs are guaranteed Date objects)
  const durationMs = differenceInMilliseconds(resolutionDate, createdDate);

  // 5. Apply Business Logic
  // CASE A: Connectivity (Limit: 3 Hours -> 10800000 ms)
  if (type.toLowerCase() === 'connectivity') {
    return durationMs <= 10800000; 
  }

  // CASE B: Solution (Limit: 6 Hours -> 21600000 ms)
  if (type.toLowerCase() === 'solution') {
     return durationMs <= 21600000;
  }

  return false;
}

// rules.constant.ts

export function calculateFcrStatus(row: any): boolean {
  // 1. Map columns safely (Trim strings to ensure " " isn't counted as value)
  const idRemedy = (row['ID Remedy_NO'] || '').toString().trim();
  const eskalasiId = (row['Eskalasi/ID Remedy_IT/AO/EMS'] || '').toString().trim();
  
  // 2. Parse MSISDN Count safely. Handle blanks as 0.
  // Using parseFloat/Int ensures we handle numbers stored as strings
  const msisdnCount = parseInt(row['Jumlah MSISDN']) || 0;

  // 3. Define "Empty" Condition
  // (In Excel, a cell might have a dash "-" to indicate empty, check your data!)
  const isIdRemedyEmpty = idRemedy === '' || idRemedy === '-';
  const isEskalasiEmpty = eskalasiId === '' || eskalasiId === '-';

  // 4. Apply FCR Logic
  // Rule: ID & Eskalasi MUST be empty AND MSISDN < 10
  if (isIdRemedyEmpty && isEskalasiEmpty && msisdnCount < 10) {
    // return 'FCR';
    return true;
  }

  // 5. Default Fallback (NON FCR)
  // This covers the inverse: ID is present OR Eskalasi is present OR MSISDN >= 10
  // return 'NON FCR';
  return false;
}

// rules.constant.ts

export function determineEskalasi(row: any): string {
  // 1. Ambil nilai kolom dengan aman dan ubah ke string
  // Kolom AL (ID Remedy_NO)
  const idRemedyNo = (row['ID Remedy_NO'] || '').toString().trim();
  
  // Kolom AM (Eskalasi/ID Remedy...)
  const eskalasiColumn = (row['Eskalasi/ID Remedy_IT/AO/EMS'] || '').toString().trim();

  // 2. Terapkan Aturan Prioritas (IF / ELSE IF)
  
  // Aturan 1: NO -> Jika Kolom AL mengandung "INC"
  if (idRemedyNo.includes('INC')) {
    return 'NO';
  }

  // Aturan 2: IT -> Jika Kolom AM mengandung "INC"
  if (eskalasiColumn.includes('INC')) {
    return 'IT';
  }

  // Aturan 3: EBO -> Jika Kolom AM mengandung "EBO"
  if (eskalasiColumn.includes('EBO')) {
    return 'EBO';
  }

  // Aturan 4: GTM -> Jika Kolom AM mengandung "GTM"
  if (eskalasiColumn.includes('GTM')) {
    return 'GTM';
  }

  // Aturan 5: Billco -> Jika Kolom AM mengandung "Billco"
  // Gunakan 'i' pada regex atau tosearch string jika ingin tidak case-sensitive, 
  // tapi di sini kita ikut persis request 'Billco'
  if (eskalasiColumn.includes('Billco')) {
    return 'Billco';
  }

  // Default jika tidak ada yang cocok (bisa kosong atau '-')
  return ''; 
}