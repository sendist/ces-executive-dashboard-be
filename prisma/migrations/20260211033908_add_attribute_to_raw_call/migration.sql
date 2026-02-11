-- AlterTable
ALTER TABLE "RawCall" ADD COLUMN     "corp" TEXT,
ADD COLUMN     "customer_type" TEXT,
ADD COLUMN     "eskalasi" TEXT,
ADD COLUMN     "inSla" BOOLEAN,
ADD COLUMN     "isFcr" BOOLEAN,
ADD COLUMN     "isPareto" BOOLEAN,
ADD COLUMN     "isVip" BOOLEAN,
ADD COLUMN     "product" TEXT,
ADD COLUMN     "project_id" TEXT,
ADD COLUMN     "statusTiket" BOOLEAN,
ADD COLUMN     "tier" TEXT,
ADD COLUMN     "validationStatus" TEXT;
