/*
  Warnings:

  - A unique constraint covering the columns `[createdAt,customer]` on the table `RawCsat` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "RawCsat_createdAt_customer_key" ON "RawCsat"("createdAt", "customer");
