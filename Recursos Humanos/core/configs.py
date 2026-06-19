from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.orm import declarative_base
from typing import ClassVar


class Settings(BaseSettings):
    """
    Configurações gerais usadas na aplicação
    """
    # Vai buscar DB_URL das variáveis de ambiente no .env
    DB_URL: str

    DBBaseModel: ClassVar[type] = declarative_base()

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()