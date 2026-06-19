from datetime import time
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Numeric, String, Text, Time, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings


class TurnoModel(settings.DBBaseModel):
    __tablename__ = "turno"
    __table_args__ = (
        CheckConstraint(
            "carga_horaria_semanal IS NULL OR carga_horaria_semanal > 0",
            name="ck_turno_carga_horaria_semanal",
        ),
        {"schema": "rh"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    nome_turno: Mapped[str] = mapped_column(
        String(80),
        nullable=False,
    )

    hora_inicio: Mapped[time] = mapped_column(
        Time,
        nullable=False,
    )

    hora_fim: Mapped[time] = mapped_column(
        Time,
        nullable=False,
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

    carga_horaria_semanal: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        nullable=True,
    )