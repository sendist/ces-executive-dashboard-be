/*
  Warnings:

  - You are about to drop the column `fcr` on the `RawOca` table. All the data in the column will be lost.
  - You are about to drop the column `sla` on the `RawOca` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "RawOca" DROP COLUMN "fcr",
DROP COLUMN "sla",
ADD COLUMN     "inSla" BOOLEAN,
ADD COLUMN     "isFcr" BOOLEAN;
