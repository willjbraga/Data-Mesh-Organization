import asyncio

from sqlalchemy import text

from core.configs import settings
from core.database import engine


async def testar_conexao():
    print("DB_URL usada:")
    print(settings.DB_URL)

    async with engine.connect() as conn:
        resultado = await conn.execute(
            text("SELECT current_database(), current_user;")
        )

        print("Resultado:")
        print(resultado.fetchone())


if __name__ == "__main__":
    asyncio.run(testar_conexao())