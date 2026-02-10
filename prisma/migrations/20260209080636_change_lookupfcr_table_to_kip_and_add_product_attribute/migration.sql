/*
  Warnings:

  - You are about to drop the `lookup_fcr` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "lookup_fcr";

-- CreateTable
CREATE TABLE "lookup_kip" (
    "id" SERIAL NOT NULL,
    "category" TEXT,
    "subCategory" TEXT,
    "detailCategory" TEXT,
    "compositeKey" TEXT,
    "fcrNonSatuan" TEXT,
    "escToSatuan" TEXT,
    "fcrNonMassal" TEXT,
    "escToMassal" TEXT,
    "isFcr" BOOLEAN,
    "product" TEXT,

    CONSTRAINT "lookup_kip_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "lookup_kip_compositeKey_idx" ON "lookup_kip"("compositeKey");

-- CreateIndex
CREATE INDEX "lookup_kip_isFcr_idx" ON "lookup_kip"("isFcr");

-- CreateIndex
CREATE INDEX "lookup_kip_product_idx" ON "lookup_kip"("product");
