/*
  Warnings:

  - A unique constraint covering the columns `[date,vector]` on the table `raw_avaya` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "raw_avaya_date_key";

-- CreateTable
CREATE TABLE "News" (
    "id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "content" JSONB NOT NULL,
    "summary" TEXT,
    "authorName" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "deletedAt" TIMESTAMP(3),

    CONSTRAINT "News_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "News_deletedAt_idx" ON "News"("deletedAt");

-- CreateIndex
CREATE UNIQUE INDEX "raw_avaya_date_vector_key" ON "raw_avaya"("date", "vector");
