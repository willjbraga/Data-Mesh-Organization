from typing import Optional

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import TipoDepartamentoEnum, pg_enum


class DepartamentoModel(settings.DBBaseModel):
    __tablename__ = "departamento"
    __table_args__ = (
        Index("idx_departamento_unidade_id", "unidade_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    nome_departamento: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    ativo: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )

    tipo_departamento: Mapped[TipoDepartamentoEnum] = mapped_column(
        pg_enum(TipoDepartamentoEnum, "tipo_departamento_enum"),
        nullable=False,
    )

    descricao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    unidade_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.unidade_restaurante.id",
            name="fk_departamento_unidade_possui",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )