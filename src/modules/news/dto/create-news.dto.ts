import { PartialType } from '@nestjs/swagger';
import { IsString, IsNotEmpty, IsObject } from 'class-validator';

export class CreateNewsDto {
  @IsString()
  @IsNotEmpty()
  title: string;

  @IsObject() // TipTap JSON object
  @IsNotEmpty()
  content: any;

  @IsString()
  @IsNotEmpty()
  authorName: string;

  @IsString()
  summary: string;
}

export class UpdateNewsDto extends PartialType(CreateNewsDto) {}
