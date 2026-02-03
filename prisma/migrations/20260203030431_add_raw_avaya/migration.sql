/*
  Warnings:

  - You are about to drop the `call_metrics` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "call_metrics";

-- CreateTable
CREATE TABLE "raw_avaya" (
    "id" SERIAL NOT NULL,
    "date" DATE NOT NULL,
    "vector" INTEGER NOT NULL,
    "inbound_calls" INTEGER NOT NULL,
    "flow_in" INTEGER NOT NULL,
    "acd_calls" INTEGER NOT NULL,
    "main_acd_calls" INTEGER NOT NULL,
    "backup_acd_calls" INTEGER NOT NULL,
    "connect_calls" INTEGER NOT NULL,
    "aban_calls" INTEGER NOT NULL,
    "flow_out" INTEGER NOT NULL,
    "forced_busy_calls" INTEGER NOT NULL,
    "forced_disc_calls" INTEGER NOT NULL,
    "acd_time" INTEGER NOT NULL,
    "hold_time" INTEGER NOT NULL,
    "aht" DECIMAL(10,4) NOT NULL,
    "avg_speed_ans" DECIMAL(10,4) NOT NULL,
    "avg_acd_time" DECIMAL(10,4) NOT NULL,
    "avg_acw_time" DECIMAL(10,4) NOT NULL,
    "avg_connect_time" DECIMAL(10,4) NOT NULL,
    "avg_aban_time" DECIMAL(10,4) NOT NULL,
    "avg_vdn_time" DECIMAL(10,4) NOT NULL,
    "percent_aban" DOUBLE PRECISION NOT NULL,
    "percent_busy" DOUBLE PRECISION NOT NULL,
    "percent_flow_out" DOUBLE PRECISION NOT NULL,
    "1st_skill_pref" INTEGER NOT NULL,
    "2nd_skill_pref" INTEGER NOT NULL,
    "3rd_skill_pref" INTEGER NOT NULL,

    CONSTRAINT "raw_avaya_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "raw_avaya_date_idx" ON "raw_avaya"("date");

-- CreateIndex
CREATE UNIQUE INDEX "raw_avaya_date_key" ON "raw_avaya"("date");
