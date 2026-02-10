export class ExcelUtils {
  static parseExcelDate(value: any): Date | null {
    if (value == null || value === '') return null;

    const WIB_OFFSET = 7 * 60 * 60 * 1000;

    // Excel serial number (Excel stores local date)
    if (typeof value === 'number') {
      return new Date(
        Date.UTC(1970, 0, 1) + (value - 25569) * 86400000 - WIB_OFFSET,
      );
    }

    if (value instanceof Date) {
      return new Date(value.getTime() - WIB_OFFSET);
    }

    if (typeof value === 'string') {
      // Handle YYYY-MM-DD HH:mm:ss
      if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
        const [datePart, timePart] = value.split(' ');

        const [year, month, day] = datePart.split('-').map(Number);
        const [hour = 0, minute = 0, second = 0] =
          timePart?.split(':').map(Number) ?? [];

        return new Date(
          Date.UTC(year, month - 1, day, hour, minute, second) - WIB_OFFSET,
        );
      }

      if (/^\d+([.,]\d+)?$/.test(value)) {
        const num = Number(value.replace(',', '.'));
        return new Date(
          Date.UTC(1970, 0, 1) + (num - 25569) * 86400000 - WIB_OFFSET,
        );
      }

      const [datePart, timePart] = value.split(' ');
      if (!datePart) return null;

      const [day, month, year] = datePart.split('/').map(Number);
      const [hour = 0, minute = 0, second = 0] =
        timePart?.split(':').map(Number) ?? [];

      return new Date(
        Date.UTC(year, month - 1, day, hour, minute, second) - WIB_OFFSET,
      );
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
