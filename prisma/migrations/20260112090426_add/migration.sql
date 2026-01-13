-- AlterTable
ALTER TABLE "RawOca" ADD COLUMN     "eskalasi" TEXT,
ADD COLUMN     "fcr" TEXT,
ADD COLUMN     "isPareto" BOOLEAN,
ADD COLUMN     "isVip" BOOLEAN;

-- CreateTable
CREATE TABLE "AccountMapping" (
    "b2b_account_id" TEXT NOT NULL,
    "corporateName" TEXT,
    "kategoriAccount" TEXT,
    "group" TEXT,
    "divisi" TEXT,
    "department" TEXT,
    "mppCodeNew" TEXT,
    "namaAM" TEXT,

    CONSTRAINT "AccountMapping_pkey" PRIMARY KEY ("b2b_account_id")
);
