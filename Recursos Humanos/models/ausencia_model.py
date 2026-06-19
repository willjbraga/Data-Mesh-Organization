from datetime import date
from typing import Optional

from sqlalchemy import BigInteger, CheckConstraint, Date, ForeignKey, Index, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import TipoAusenciaEnum, StatusAprovacaoEnum, pg_enum


class AusenciaModel(settings.DBBaseModel):
    __tablename__ = "ausencia"
    __table_args__ = (
        CheckConstraint(
            "data_fim >= data_inicio",
            name="ck_ausencia_periodo",
        ),
        Index("idx_ausencia_colaborador_id", "colaborador_id"),
        Index("idx_ausencia_aprovador_id", "aprovador_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    tipo_ausencia: Mapped[TipoAusenciaEnum] = mapped_column(
        pg_enum(TipoAusenciaEnum, "tipo_ausencia_enum"),
        nullable=False,
    )

    status_aprovacao: Mapped[StatusAprovacaoEnum] = mapped_column(
        pg_enum(StatusAprovacaoEnum, "status_aprovacao_enum"),
        nullable=False,
    )

    motivo: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    observacao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    data_aprovacao: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    data_solicitacao: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        server_default=text("current_date"),
    )

    data_inicio: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    data_fim: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    colaborador_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.colaborador.id",
            name="fk_ausencia_colaborador_registra",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    aprovador_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.colaborador.id",
            name="fk_ausencia_colaborador_aprova",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )