-- CreateTable
CREATE TABLE "OcaDailySync" (
    "id" SERIAL NOT NULL,
    "lastSync" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OcaDailySync_pkey" PRIMARY KEY ("id")
);
