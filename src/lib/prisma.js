import pkg from '@prisma/client';
const { PrismaClient } = pkg;
import { PrismaBetterSqlite3 } from '@prisma/adapter-better-sqlite3';
import 'dotenv/config';

// Initialize Prisma adapter with configuration object
const adapter = new PrismaBetterSqlite3({
    url: process.env.DATABASE_URL || 'file:./database.sqlite'
});

// Export instantiated PrismaClient
export const prisma = new PrismaClient({ adapter });

export default prisma;
