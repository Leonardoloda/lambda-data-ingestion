from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String

from base import Base

class Crew(Base):
    __tablename__ = "Crew"

    id: Mapped[str] = mapped_column(String(10), primary_key=True, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    rank: Mapped[str]  = mapped_column(String(5), nullable=False)
    base: Mapped[str] = mapped_column(String(5), nullable=False)
    category: Mapped[str] = mapped_column(String(10), nullable=False)
    employer: Mapped[str] = mapped_column(String(5), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
