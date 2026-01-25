export class ExcelUtils {
 static parseExcelDate(value: any): Date | null {
    if (!value) return null;

    // --- CASE 1 & 2: Numeric or Already a Date Object ---
    // If it's a number, we convert to Date first.
    if (typeof value === 'number' || (typeof value === 'string' && !isNaN(Number(value.replace(',', '.'))))) {
        const num = typeof value === 'number' ? value : Number(value.replace(',', '.'));
        return new Date((num - 25569) * 86400000);
    }

    if (value instanceof Date) {
        // If it's already a Date, it might be in WIB. 
        // We extract the "local" parts and rebuild it as UTC.
        return new Date(Date.UTC(
            value.getFullYear(),
            value.getMonth(),
            value.getDate(),
            value.getHours(),
            value.getMinutes(),
            value.getSeconds()
        ));
    }

    // --- CASE 3: String Parsing ("DD/MM/YYYY HH:mm:ss") ---
    if (typeof value === 'string') {
        const [datePart, timePart] = value.split(' ');
        if (!datePart) return null;

        const [day, month, year] = datePart.split('/').map(num => parseInt(num));
        const [hour, minute, second] = timePart 
            ? timePart.split(':').map(num => parseInt(num)) 
            : [0, 0, 0];

        // Create the date strictly in UTC
        return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
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