from datetime import date
from typing import Optional

from sqlalchemy import BigInteger, Date, String, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from core.configs import settings


class CandidatoModel(settings.DBBaseModel):
    __tablename__ = "candidato"
    __table_args__ = (
        UniqueConstraint("email", name="uq_candidato_email"),
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

    email: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
    )

    telefone: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
    )

    data_cadastro: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        server_default=text("current_date"),
    )