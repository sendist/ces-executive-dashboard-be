-- CreateTable
CREATE TABLE "RawCsat" (
    "id" SERIAL NOT NULL,
    "interactionId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL,
    "answeredAt" TIMESTAMP(3),
    "status" TEXT,
    "ticketNumbers" TEXT,
    "numericScore" INTEGER,
    "sourceFile" TEXT NOT NULL,

    CONSTRAINT "RawCsat_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DailyCsatStat" (
    "id" SERIAL NOT NULL,
    "date" TIMESTAMP(3) NOT NULL,
    "totalSurvey" INTEGER NOT NULL DEFAULT 0,
    "totalDijawab" INTEGER NOT NULL DEFAULT 0,
    "totalJawaban45" INTEGER NOT NULL DEFAULT 0,
    "persenCsat" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "scoreCsat" DOUBLE PRECISION NOT NULL DEFAULT 0,

    CONSTRAINT "DailyCsatStat_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "RawCsat_interactionId_key" ON "RawCsat"("interactionId");

-- CreateIndex
CREATE INDEX "RawCsat_createdAt_idx" ON "RawCsat"("createdAt");

-- CreateIndex
CREATE INDEX "RawCsat_numericScore_idx" ON "RawCsat"("numericScore");

-- CreateIndex
CREATE UNIQUE INDEX "DailyCsatStat_date_key" ON "DailyCsatStat"("date");
