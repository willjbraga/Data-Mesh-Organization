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
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import (
    OrigemCandidaturaEnum,
    StatusCandidaturaEnum,
    EtapaCandidaturaEnum,
    pg_enum,
)


class CandidaturaModel(settings.DBBaseModel):
    __tablename__ = "candidatura"
    __table_args__ = (
        UniqueConstraint(
            "vaga_id",
            "candidato_id",
            name="uq_candidatura_vaga_candidato",
        ),
        CheckConstraint(
            "pretensao_salarial IS NULL OR pretensao_salarial >= 0",
            name="ck_candidatura_pretensao_salarial",
        ),
        Index("idx_candidatura_vaga_id", "vaga_id"),
        Index("idx_candidatura_candidato_id", "candidato_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    data_candidatura: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        server_default=text("current_date"),
    )

    origem_candidatura: Mapped[OrigemCandidaturaEnum] = mapped_column(
        pg_enum(OrigemCandidaturaEnum, "origem_candidatura_enum"),
        nullable=False,
    )

    status_candidatura: Mapped[StatusCandidaturaEnum] = mapped_column(
        pg_enum(StatusCandidaturaEnum, "status_candidatura_enum"),
        nullable=False,
    )

    etapa_atual: Mapped[EtapaCandidaturaEnum] = mapped_column(
        pg_enum(EtapaCandidaturaEnum, "etapa_candidatura_enum"),
        nullable=False,
    )

    observacao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    pretensao_salarial: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    vaga_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.recrutamento_vaga.id",
            name="fk_candidatura_recrutamento_vaga_recebe",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    candidato_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.candidato.id",
            name="fk_candidatura_candidato_realiza",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )