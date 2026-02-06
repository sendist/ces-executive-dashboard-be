-- CreateTable
CREATE TABLE "lookup_fcr" (
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

    CONSTRAINT "lookup_fcr_pkey" PRIMARY KEY ("id")
);
