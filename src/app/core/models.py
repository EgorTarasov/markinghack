from typing import Optional
from datetime import datetime
from sqlalchemy.types import String, Integer, DateTime
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey

from app.core.database import Base


class User(Base):
    """Производитель товаров"""

    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64))
    password: Mapped[str] = mapped_column(String())
    email: Mapped[str] = mapped_column(String())
    fullname: Mapped[Optional[str]]

    items: Mapped[list["Item"]] = relationship(back_populates="user")
    files: Mapped[list["UserFile"]] = relationship(back_populates="user")
    produced_goods: Mapped[list["ProducedGoods"]] = relationship(back_populates="user")
    sold_goods: Mapped[list["SoldGoods"]] = relationship(back_populates="user")
    transported_goods: Mapped[list["TransportedGoods"]] = relationship(
        back_populates="user"
    )


class Item(Base):
    __tablename__ = "item"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64))

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship(back_populates="items")


class UserFile(Base):
    __tablename__ = "user_files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String(64))
    path: Mapped[str] = mapped_column(String(512))
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="files")

    def __repr__(self) -> str:
        return f"UserFile(id={self.id}, filename={self.filename}, path={self.path})"


class ProducedGoods(Base):
    """Данные о вводе товаров в оборот с 2021-11-22 по 2022-11-21 один производитель"""

    __tablename__ = "produced_goods"
    # "dt","inn","gtin","prid","operation_type","cnt"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    inn: Mapped[str] = mapped_column(String(64))
    gtin: Mapped[str] = mapped_column(String(64))
    prid: Mapped[str] = mapped_column(String(64))
    operation_type: Mapped[str] = mapped_column(String(64))
    cnt: Mapped[int] = mapped_column(Integer())
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="produced_goods")


class SoldGoods(Base):
    """Данные о выводе товаров из оборота с 2021-11-22 по 2022-11-21 один производитель"""

    __tablename__ = "sold_goods"

    # "dt","gtin","prid","inn","id_sp_","type_operation","price","cnt"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    gtin: Mapped[str] = mapped_column(String(64))
    prid: Mapped[str] = mapped_column(String(64))
    inn: Mapped[str] = mapped_column(String(64))
    id_sp_: Mapped[str] = mapped_column(String(256))
    type_operation: Mapped[str] = mapped_column(String(64))
    price: Mapped[int] = mapped_column(Integer())
    cnt: Mapped[int] = mapped_column(Integer())

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="sold_goods")


class TransportedGoods(Base):
    """Данные о перемещениях товаров между участниками с 2021-11-22 по 2022-11-21 один производитель"""

    __tablename__ = "transported_goods"

    # "dt","gtin","prid","sender_inn","receiver_inn","cnt_moved"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    gtin: Mapped[str] = mapped_column(String(64))
    prid: Mapped[str] = mapped_column(String(64))
    sender_inn: Mapped[str] = mapped_column(String(64))
    receiver_inn: Mapped[str] = mapped_column(String(64))
    cnt_moved: Mapped[int] = mapped_column(Integer)

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="transported_goods")
