from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Numeric, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import TipoCargoEnum, pg_enum


class CargoModel(settings.DBBaseModel):
    __tablename__ = "cargo"
    __table_args__ = (
        CheckConstraint(
            "salario_base IS NULL OR salario_base >= 0",
            name="ck_cargo_salario_base",
        ),
        CheckConstraint(
            "faixa_salarial_min IS NULL OR faixa_salarial_min >= 0",
            name="ck_cargo_faixa_salarial_min",
        ),
        CheckConstraint(
            "faixa_salarial_max IS NULL OR faixa_salarial_max >= 0",
            name="ck_cargo_faixa_salarial_max",
        ),
        CheckConstraint(
            """
            faixa_salarial_min IS NULL
            OR faixa_salarial_max IS NULL
            OR faixa_salarial_min <= faixa_salarial_max
            """,
            name="ck_cargo_faixa_salarial_valida",
        ),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    nome_cargo: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    nivel: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
    )

    descricao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    tipo_cargo: Mapped[TipoCargoEnum] = mapped_column(
        pg_enum(TipoCargoEnum, "tipo_cargo_enum"),
        nullable=False,
    )

    ativo: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )

    salario_base: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    faixa_salarial_min: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    faixa_salarial_max: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )