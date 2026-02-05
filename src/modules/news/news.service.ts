import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { CreateNewsDto, UpdateNewsDto } from './dto/create-news.dto';
import { QueryNewsDto } from './dto/query-news.dto';
import { Prisma } from '@prisma/client';

@Injectable()
export class NewsService {
  constructor(private prisma: PrismaService) {}

  async create(dto: CreateNewsDto) {
    return this.prisma.news.create({
      data: dto,
    });
  }

  async findAll(query: QueryNewsDto) {
    const { search } = query;

    const page = Number(query.page || 1);
    const limit = Number(query.limit || 10);

    // Calculate how many records to skip
    const skip = (page - 1) * limit;

    // Build the search filter
    const searchFilter = search
      ? {
          OR: [
            { title: { contains: search, mode: Prisma.QueryMode.insensitive } },
            {
              summary: { contains: search, mode: Prisma.QueryMode.insensitive },
            },
          ],
        }
      : {};

    const where: Prisma.NewsWhereInput = {
      deletedAt: null,
      ...searchFilter,
    };

    // Execute both count and data fetch in parallel for better performance
    const [total, data] = await Promise.all([
      this.prisma.news.count({ where }),
      this.prisma.news.findMany({
        where,
        skip,
        take: limit,
        orderBy: { createdAt: 'desc' },
      }),
    ]);

    return {
      data,
      meta: {
        total,
        page,
        lastPage: Math.ceil(total / limit),
      },
    };
  }
  async findOne(id: string) {
    const news = await this.prisma.news.findFirst({
      where: { id, deletedAt: null },
    });
    if (!news) throw new NotFoundException('News article not found');
    return news;
  }

  async update(id: string, dto: UpdateNewsDto) {
    await this.findOne(id); // Ensure it exists and isn't deleted
    return this.prisma.news.update({
      where: { id },
      data: dto,
    });
  }

  async remove(id: string) {
    await this.findOne(id);
    return this.prisma.news.update({
      where: { id },
      data: { deletedAt: new Date() }, // Soft delete
    });
  }
}
