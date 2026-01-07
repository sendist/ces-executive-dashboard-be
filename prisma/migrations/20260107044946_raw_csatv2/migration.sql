/*
  Warnings:

  - The primary key for the `RawCsat` table will be changed. If it partially fails, the table could be left without primary key constraint.

*/
-- DropIndex
DROP INDEX "RawCsat_interactionId_key";

-- AlterTable
ALTER TABLE "RawCsat" DROP CONSTRAINT "RawCsat_pkey",
ALTER COLUMN "id" DROP DEFAULT,
ALTER COLUMN "id" SET DATA TYPE VARCHAR(24),
ADD CONSTRAINT "RawCsat_pkey" PRIMARY KEY ("id");
DROP SEQUENCE "RawCsat_id_seq";
