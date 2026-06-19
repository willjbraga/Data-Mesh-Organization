from typing import Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import TipoTreinamentoEnum, pg_enum


class TreinamentoModel(settings.DBBaseModel):
    __tablename__ = "treinamento"
    __table_args__ = (
        CheckConstraint(
            "validade_meses IS NULL OR validade_meses >= 0",
            name="ck_treinamento_validade_meses",
        ),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    nome_treinamento: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
    )

    tipo_treinamento: Mapped[TipoTreinamentoEnum] = mapped_column(
        pg_enum(TipoTreinamentoEnum, "tipo_treinamento_enum"),
        nullable=False,
    )

    obrigatorio: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )

    validade_meses: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
    )

    descricao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    ativo: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )