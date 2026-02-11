/*
  Warnings:

  - You are about to drop the column `APP_ID` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `AREA_NAME` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `BRAND` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `EMPLOYEE_CODE` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `EMPLOYEE_NAME` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `MSISDN` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `NOTES` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `REG_NAME` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `SERVICE` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `TOPIC_REASON_1` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `TOPIC_REASON_2` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `TOPIC_RESULT` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `UNIT_NAME` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `UNIT_TYPE` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `USER_ID` on the `RawCall` table. All the data in the column will be lost.
  - You are about to drop the column `Update_Stamp` on the `RawCall` table. All the data in the column will be lost.
  - A unique constraint covering the columns `[update_stamp,msisdn]` on the table `RawCall` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "RawCall_MSISDN_idx";

-- DropIndex
DROP INDEX "RawCall_Update_Stamp_MSISDN_key";

-- DropIndex
DROP INDEX "RawCall_Update_Stamp_idx";

-- AlterTable
ALTER TABLE "RawCall" DROP COLUMN "APP_ID",
DROP COLUMN "AREA_NAME",
DROP COLUMN "BRAND",
DROP COLUMN "EMPLOYEE_CODE",
DROP COLUMN "EMPLOYEE_NAME",
DROP COLUMN "MSISDN",
DROP COLUMN "NOTES",
DROP COLUMN "REG_NAME",
DROP COLUMN "SERVICE",
DROP COLUMN "TOPIC_REASON_1",
DROP COLUMN "TOPIC_REASON_2",
DROP COLUMN "TOPIC_RESULT",
DROP COLUMN "UNIT_NAME",
DROP COLUMN "UNIT_TYPE",
DROP COLUMN "USER_ID",
DROP COLUMN "Update_Stamp",
ADD COLUMN     "app_id" TEXT,
ADD COLUMN     "area_name" TEXT,
ADD COLUMN     "brand" TEXT,
ADD COLUMN     "employee_code" TEXT,
ADD COLUMN     "employee_name" TEXT,
ADD COLUMN     "msisdn" TEXT,
ADD COLUMN     "notes" TEXT,
ADD COLUMN     "reg_name" TEXT,
ADD COLUMN     "service" TEXT,
ADD COLUMN     "topic_reason_1" TEXT,
ADD COLUMN     "topic_reason_2" TEXT,
ADD COLUMN     "topic_result" TEXT,
ADD COLUMN     "unit_name" TEXT,
ADD COLUMN     "unit_type" TEXT,
ADD COLUMN     "update_stamp" TIMESTAMP(3),
ADD COLUMN     "user_id" TEXT;

-- CreateIndex
CREATE INDEX "RawCall_msisdn_idx" ON "RawCall"("msisdn");

-- CreateIndex
CREATE INDEX "RawCall_update_stamp_idx" ON "RawCall"("update_stamp");

-- CreateIndex
CREATE UNIQUE INDEX "RawCall_update_stamp_msisdn_key" ON "RawCall"("update_stamp", "msisdn");
