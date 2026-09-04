import { PrismaPg } from "@prisma/adapter-pg";
import { config } from ".";
import { PrismaClient } from "@prisma/client/extension";

const connectionString = config.DATABASE_URL;

const globalForPrisma = global
if (!globalForPrisma.prisma) {
  const adapter = new PrismaPg({ connectionString })

  globalForPrisma.prisma = new PrismaClient({
    adapter,
    log:['error','warn']
  })
}
const prisma = globalForPrisma.prisma

export default prisma