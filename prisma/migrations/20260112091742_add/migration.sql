/*
  Warnings:

  - The primary key for the `AccountMapping` table will be changed. If it partially fails, the table could be left without primary key constraint.
  - A unique constraint covering the columns `[b2b_account_id]` on the table `AccountMapping` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "AccountMapping" DROP CONSTRAINT "AccountMapping_pkey",
ADD COLUMN     "id" SERIAL NOT NULL,
ADD CONSTRAINT "AccountMapping_pkey" PRIMARY KEY ("id");

-- CreateIndex
CREATE UNIQUE INDEX "AccountMapping_b2b_account_id_key" ON "AccountMapping"("b2b_account_id");

-- CreateIndex
CREATE INDEX "AccountMapping_corporateName_idx" ON "AccountMapping"("corporateName");

-- CreateIndex
CREATE INDEX "AccountMapping_kategoriAccount_idx" ON "AccountMapping"("kategoriAccount");
