import { Module, Global } from '@nestjs/common';
import { PrismaService } from './prisma.service';

@Global() // This makes PrismaService available everywhere without importing the module again
@Module({
  providers: [PrismaService],
  exports: [PrismaService],
})
export class PrismaModule {}