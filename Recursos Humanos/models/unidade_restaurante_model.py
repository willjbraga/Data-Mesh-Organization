from sqlalchemy import BigInteger, CHAR, CheckConstraint, String
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import TipoUnidadeEnum, StatusUnidadeEnum, pg_enum


class UnidadeRestauranteModel(settings.DBBaseModel):
    __tablename__ = "unidade_restaurante"
    __table_args__ = (
        CheckConstraint(
            "estado ~ '^[A-Z]{2}$'",
            name="ck_unidade_restaurante_estado",
        ),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    nome_unidade: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    tipo_unidade: Mapped[TipoUnidadeEnum] = mapped_column(
        pg_enum(TipoUnidadeEnum, "tipo_unidade_enum"),
        nullable=False,
    )

    cidade: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    estado: Mapped[str] = mapped_column(
        CHAR(2),
        nullable=False,
    )

    status: Mapped[StatusUnidadeEnum] = mapped_column(
        pg_enum(StatusUnidadeEnum, "status_unidade_enum"),
        nullable=False,
    )