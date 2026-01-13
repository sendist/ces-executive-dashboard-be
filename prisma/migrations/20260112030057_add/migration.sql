-- AlterTable
ALTER TABLE "RawOca" ADD COLUMN     "product" TEXT,
ADD COLUMN     "sla" TEXT,
ADD COLUMN     "statusTiket" BOOLEAN,
ADD COLUMN     "validationStatus" TEXT;

-- CreateTable
CREATE TABLE "KIP" (
    "id" SERIAL NOT NULL,
    "category" TEXT,
    "subCategory" TEXT,
    "detailCategory" TEXT,
    "product" TEXT,
    "subProduct" TEXT,

    CONSTRAINT "KIP_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "KIP_subCategory_idx" ON "KIP"("subCategory");

-- CreateIndex
CREATE INDEX "KIP_product_idx" ON "KIP"("product");
