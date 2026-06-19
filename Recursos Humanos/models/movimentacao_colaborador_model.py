from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, CheckConstraint, Date, ForeignKey, Index, Numeric, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import TipoMovimentacaoEnum, pg_enum


class MovimentacaoColaboradorModel(settings.DBBaseModel):
    __tablename__ = "movimentacao_colaborador"
    __table_args__ = (
        CheckConstraint(
            "salario_anterior IS NULL OR salario_anterior >= 0",
            name="ck_movimentacao_salario_anterior",
        ),
        CheckConstraint(
            "salario_novo IS NULL OR salario_novo >= 0",
            name="ck_movimentacao_salario_novo",
        ),
        Index("idx_movimentacao_colaborador_id", "colaborador_id"),
        Index("idx_movimentacao_cargo_anterior_id", "cargo_anterior_id"),
        Index("idx_movimentacao_cargo_novo_id", "cargo_novo_id"),
        Index("idx_movimentacao_departamento_origem_id", "departamento_origem_id"),
        Index("idx_movimentacao_departamento_destino_id", "departamento_destino_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    tipo_movimentacao: Mapped[TipoMovimentacaoEnum] = mapped_column(
        pg_enum(TipoMovimentacaoEnum, "tipo_movimentacao_enum"),
        nullable=False,
    )

    data_movimentacao: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        server_default=text("current_date"),
    )

    data_efetivacao: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    salario_novo: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    salario_anterior: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    motivo: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    observacao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    colaborador_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.colaborador.id",
            name="fk_movimentacao_colaborador_colaborador_associada",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    cargo_anterior_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.cargo.id",
            name="fk_movimentacao_colaborador_cargo_tinha",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )

    cargo_novo_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.cargo.id",
            name="fk_movimentacao_colaborador_cargo_assume",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )

    departamento_origem_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.departamento.id",
            name="fk_movimentacao_colaborador_departamento_sai_de",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )

    departamento_destino_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.departamento.id",
            name="fk_movimentacao_colaborador_departamento_vai_para",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )