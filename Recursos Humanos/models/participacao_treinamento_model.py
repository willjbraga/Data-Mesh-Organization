from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    ForeignKey,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import StatusParticipacaoTreinamentoEnum, pg_enum


class ParticipacaoTreinamentoModel(settings.DBBaseModel):
    __tablename__ = "participacao_treinamento"
    __table_args__ = (
        UniqueConstraint(
            "colaborador_id",
            "treinamento_id",
            "data_realizacao",
            name="uq_participacao_treinamento_colab_trein_data",
        ),
        CheckConstraint(
            "nota IS NULL OR nota BETWEEN 0 AND 100",
            name="ck_participacao_treinamento_nota",
        ),
        Index("idx_participacao_treinamento_colaborador_id", "colaborador_id"),
        Index("idx_participacao_treinamento_treinamento_id", "treinamento_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    status: Mapped[StatusParticipacaoTreinamentoEnum] = mapped_column(
        pg_enum(
            StatusParticipacaoTreinamentoEnum,
            "status_participacao_treinamento_enum",
        ),
        nullable=False,
    )

    data_validade: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    nota: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        nullable=True,
    )

    data_realizacao: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    observacao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    colaborador_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.colaborador.id",
            name="fk_participacao_treinamento_colaborador_participa",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    treinamento_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.treinamento.id",
            name="fk_participacao_treinamento_treinamento_refere_se",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )