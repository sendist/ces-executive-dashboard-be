/*
  Warnings:

  - You are about to alter the column `amount_revenue` on the `RawOca` table. The data in that column could be lost. The data in that column will be cast from `DoublePrecision` to `BigInt`.

*/
-- AlterTable
ALTER TABLE "RawOca" ALTER COLUMN "amount_revenue" SET DATA TYPE BIGINT,
ALTER COLUMN "jumlah_msisdn" SET DATA TYPE TEXT;
