from prisma import Prisma
import asyncio

async def DbConnection():
    db = Prisma()
    await db.connect()
    dados = await db.inventario.find_many()
    print(dados)
    await db.disconnect()

asyncio.run(DbConnection())