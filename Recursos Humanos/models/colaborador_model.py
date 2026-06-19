from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    CHAR,
    CheckConstraint,
    Date,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings
from models.enums import StatusColaboradorEnum, TipoVinculoEnum, pg_enum


class ColaboradorModel(settings.DBBaseModel):
    __tablename__ = "colaborador"
    __table_args__ = (
        UniqueConstraint("cpf", name="uq_colaborador_cpf"),
        UniqueConstraint("matricula", name="uq_colaborador_matricula"),
        UniqueConstraint("email", name="uq_colaborador_email"),
        CheckConstraint(
            "cpf ~ '^[0-9]{11}$'",
            name="ck_colaborador_cpf",
        ),
        CheckConstraint(
            "salario_atual IS NULL OR salario_atual >= 0",
            name="ck_colaborador_salario_atual",
        ),
        CheckConstraint(
            "data_desligamento IS NULL OR data_desligamento >= data_admissao",
            name="ck_colaborador_data_desligamento",
        ),
        CheckConstraint(
            "gerente_id IS NULL OR gerente_id <> id",
            name="ck_colaborador_gerente_diferente",
        ),
        Index("idx_colaborador_gerente_id", "gerente_id"),
        Index("idx_colaborador_cargo_id", "cargo_id"),
        Index("idx_colaborador_departamento_id", "departamento_id"),
        Index("idx_colaborador_turno_id", "turno_id"),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    nome: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
    )

    telefone: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
    )

    data_admissao: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    status: Mapped[StatusColaboradorEnum] = mapped_column(
        pg_enum(StatusColaboradorEnum, "status_colaborador_enum"),
        nullable=False,
    )

    tipo_vinculo: Mapped[TipoVinculoEnum] = mapped_column(
        pg_enum(TipoVinculoEnum, "tipo_vinculo_enum"),
        nullable=False,
    )

    email: Mapped[Optional[str]] = mapped_column(
        String(200),
        nullable=True,
    )

    cpf: Mapped[str] = mapped_column(
        CHAR(11),
        nullable=False,
    )

    matricula: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    data_nascimento: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    salario_atual: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )

    data_desligamento: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    observacao: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    gerente_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.colaborador.id",
            name="fk_colaborador_colaborador_gerencia",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )

    cargo_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.cargo.id",
            name="fk_colaborador_cargo_ocupa",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    departamento_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.departamento.id",
            name="fk_colaborador_departamento_alocado",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    turno_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey(
            "rh.turno.id",
            name="fk_colaborador_turno_cumpre",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )