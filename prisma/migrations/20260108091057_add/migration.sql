-- CreateTable
CREATE TABLE "RawOca" (
    "id" SERIAL NOT NULL,
    "ticket_number" TEXT NOT NULL,
    "ticket_subject" TEXT,
    "channel" TEXT,
    "category" TEXT,
    "reporter" TEXT,
    "assignee" TEXT,
    "department" TEXT,
    "priority" TEXT,
    "last_status" TEXT,
    "ticket_created" TIMESTAMP(3),
    "last_update" TIMESTAMP(3),
    "description" TEXT,
    "customer_name" TEXT,
    "customer_phone" TEXT,
    "customer_address" TEXT,
    "customer_email" TEXT,
    "first_response_time" TIMESTAMP(3),
    "total_response_time" TEXT,
    "total_resolution_time" TEXT,
    "resolve_time" TIMESTAMP(3),
    "resolved_by" TEXT,
    "closed_time" TIMESTAMP(3),
    "ticket_duration" TEXT,
    "count_inbound_message" INTEGER,
    "label_in_room" TEXT,
    "first_response_duration" TEXT,
    "escalate_ticket" TEXT,
    "last_assignee_escalation" TEXT,
    "last_status_escalation" TEXT,
    "last_update_escalation" TEXT,
    "converse" TEXT,
    "move_to_other_channel" TEXT,
    "previous_channel" TEXT,
    "amount_revenue" DOUBLE PRECISION,
    "jumlah_msisdn" INTEGER,
    "tags" TEXT,
    "id_remedy_no" TEXT,
    "eskalasi_id_remedy_it_ao_ems" TEXT,
    "reason_osl" TEXT,
    "project_id" TEXT,
    "nama_perusahaan" TEXT,
    "roaming" TEXT,
    "sub_category" TEXT,
    "detail_category" TEXT,
    "iot" TEXT,
    "updated_at_excel" TIMESTAMP(3),

    CONSTRAINT "RawOca_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "RawOca_ticket_number_key" ON "RawOca"("ticket_number");

-- CreateIndex
CREATE INDEX "RawOca_ticket_number_idx" ON "RawOca"("ticket_number");

-- CreateIndex
CREATE INDEX "RawOca_ticket_created_idx" ON "RawOca"("ticket_created");

-- CreateIndex
CREATE INDEX "RawOca_last_update_idx" ON "RawOca"("last_update");
