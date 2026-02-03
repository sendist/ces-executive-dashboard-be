-- CreateTable
CREATE TABLE "call_metrics" (
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

    CONSTRAINT "call_metrics_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "call_metrics_date_idx" ON "call_metrics"("date");

-- CreateIndex
CREATE UNIQUE INDEX "call_metrics_date_key" ON "call_metrics"("date");

-- CreateIndex
CREATE INDEX "idx_rawoca_channel_created" ON "RawOca"("channel", "ticket_created");

-- CreateIndex
CREATE INDEX "idx_rawoca_company" ON "RawOca"("nama_perusahaan");

-- CreateIndex
CREATE INDEX "idx_rawoca_detail_category" ON "RawOca"("detail_category");

-- CreateIndex
CREATE INDEX "idx_rawoca_eskalasi_created" ON "RawOca"("eskalasi", "ticket_created");

-- CreateIndex
CREATE INDEX "idx_rawoca_product_created" ON "RawOca"("product", "ticket_created");

-- CreateIndex
CREATE INDEX "idx_rawomnix_channel_created" ON "RawOmnix"("channel_name", "date_start_interaction");

-- CreateIndex
CREATE INDEX "idx_rawomnix_company" ON "RawOmnix"("ticket_perusahaan");

-- CreateIndex
CREATE INDEX "idx_rawomnix_detail_category" ON "RawOmnix"("subCategory");

-- CreateIndex
CREATE INDEX "idx_rawomnix_product_created" ON "RawOmnix"("product", "date_start_interaction");
