/*
  Warnings:

  - You are about to drop the column `numericScore` on the `RawCsat` table. All the data in the column will be lost.
  - You are about to drop the column `sourceFile` on the `RawCsat` table. All the data in the column will be lost.

*/
-- DropIndex
DROP INDEX "RawCsat_numericScore_idx";

-- AlterTable
ALTER TABLE "RawCsat" DROP COLUMN "numericScore",
DROP COLUMN "sourceFile",
ADD COLUMN     "assignedAgent" TEXT,
ADD COLUMN     "channel" TEXT,
ADD COLUMN     "customer" TEXT,
ADD COLUMN     "numeric" INTEGER,
ADD COLUMN     "question1" TEXT,
ADD COLUMN     "question2" TEXT,
ADD COLUMN     "question3" TEXT,
ADD COLUMN     "question4" TEXT,
ADD COLUMN     "question5" TEXT,
ADD COLUMN     "question6" TEXT;
