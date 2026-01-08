-- CreateTable
CREATE TABLE "RawCall" (
    "id" SERIAL NOT NULL,
    "Update_Stamp" TIMESTAMP(3),
    "MSISDN" TEXT,
    "BRAND" TEXT,
    "UNIT_TYPE" TEXT,
    "UNIT_NAME" TEXT,
    "AREA_NAME" TEXT,
    "REG_NAME" TEXT,
    "TOPIC_REASON_1" TEXT,
    "TOPIC_REASON_2" TEXT,
    "TOPIC_RESULT" TEXT,
    "SERVICE" TEXT,
    "APP_ID" TEXT,
    "USER_ID" TEXT,
    "EMPLOYEE_CODE" TEXT,
    "EMPLOYEE_NAME" TEXT,
    "NOTES" TEXT,

    CONSTRAINT "RawCall_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "RawCall_MSISDN_idx" ON "RawCall"("MSISDN");

-- CreateIndex
CREATE INDEX "RawCall_Update_Stamp_idx" ON "RawCall"("Update_Stamp");

-- CreateIndex
CREATE UNIQUE INDEX "RawCall_Update_Stamp_MSISDN_key" ON "RawCall"("Update_Stamp", "MSISDN");
