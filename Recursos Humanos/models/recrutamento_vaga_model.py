from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, CheckConstraint, Date, ForeignKey, Index, Integer, Numeric, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import StatusVagaEnum, TipoVinculoEnum, pg_enum


class RecrutamentoVagaModel(settings.DBBaseModel):
    __tablename__ = "recrutamento_vaga"
    __table_args__ = (
        CheckConstraint(
            "quantidade_vagas > 0",
            name="ck_recrutamento_vaga_quantidade",
        ),
        CheckConstraint(
            "salario_ofertado IS NULL OR salario_ofertado >= 0",
            name="ck_recrutamento_vaga_salario",
        ),
        CheckConstraint(
            "data_fechamento IS NULL OR data_fechamento >= data_abertura",
            name="ck_recrutamento_vaga_datas",
        ),
        Index("idx_recrutamento_vaga_departamento_id", "departamento_id"),
        Index("idx_recrutamento_vaga_turno_id", "turno_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    titulo: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
    )

    descricao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    requisitos: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    data_abertura: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        server_default=text("current_date"),
    )

    data_fechamento: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    status: Mapped[StatusVagaEnum] = mapped_column(
        pg_enum(StatusVagaEnum, "status_vaga_enum"),
        nullable=False,
    )

    quantidade_vagas: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    tipo_vinculo: Mapped[TipoVinculoEnum] = mapped_column(
        pg_enum(TipoVinculoEnum, "tipo_vinculo_enum"),
        nullable=False,
    )

    salario_ofertado: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    departamento_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.departamento.id",
            name="fk_recrutamento_vaga_departamento_oferta",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    turno_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.turno.id",
            name="fk_recrutamento_vaga_turno_preve",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )