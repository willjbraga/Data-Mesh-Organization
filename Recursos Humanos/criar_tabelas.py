from core.configs import settings
from core.database import engine
from sqlalchemy import text

async def create_tables() -> None:
    import models.__allmodels
    print('Criando as tabelas no banco de dados...')

    async with engine.begin() as conn:
        print("Criando schema rh...")
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS rh"))
        await conn.run_sync(settings.DBBaseModel.metadata.drop_all)
        await conn.run_sync(settings.DBBaseModel.metadata.create_all)

    print('Tabelas criadas com sucesso.')

if __name__ == '__main__':
    import asyncio

    asyncio.run(create_tables())