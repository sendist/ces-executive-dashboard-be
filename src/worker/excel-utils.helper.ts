export class ExcelUtils {
    static parseExcelDate(value) {
      if (!value) return null;
// --- NEW: Pre-processing for String Numbers (e.g., "46006,7186") ---
    // If it's a string that looks like a number (allows both . and , as decimals)
    if (typeof value === 'string') {
        const normalized = value.replace(',', '.');
        // Check if it is a valid number and NOT a date string like "01/01/2025"
        // (Date strings with slashes usually result in NaN when cast to Number directly)
        if (!isNaN(Number(normalized)) && normalized.trim() !== '') {
            value = Number(normalized);
        }
    }
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

  // --- HELPER: Parse Int/Float safely ---
  static parseNumber(value: any): number | null {
    if (!value) return null;
    // Remove "Rp", ",", etc if necessary, but usually simple parseInt works
    const num = Number(value);
    return isNaN(num) ? null : num;
  }

  // Helper for Big Integers (Revenue)
  // Turns "13513500000", "-", "~" into BigInt or Null
  static parseSafeBigInt(value: any): BigInt | null {
    if (!value) return null;
    let str = value.toString().trim();
    
    // Clean garbage characters
    if (str === '-' || str === '~' || str === '') return null;
    
    // Remove non-numeric chars (keep digits and minus sign)
    str = str.replace(/[^0-9-]/g, '');
    
    try {
      return BigInt(str);
    } catch {
      return null;
    }
  }

  // Helper for Standard Integers (Counts)
  // Turns "30", "-", "~" into Number or Null
  static parseSafeInt(value: any): number | null {
    if (!value) return null;
    let str = value.toString().trim();
    
    if (str === '-' || str === '~' || str === '') return null;
    
    const num = parseInt(str);
    return isNaN(num) ? null : num;
  }

  // Helper to safely format values for Raw SQL
  static formatSqlValue = (value: any): string => {
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
}