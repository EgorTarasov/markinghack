from typing import Optional, Any
from datetime import datetime
from sqlalchemy.types import String, Integer, DateTime, BigInteger
from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey
import numpy as np

from app.core.database import Base

# region service data
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
    agg_produced: Mapped[list["AgrProduction"]] = relationship(back_populates="user")
    agr_sold: Mapped[list["AgrSold"]] = relationship(back_populates="user")


class Item(Base):
    __tablename__ = "item"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64))

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship(back_populates="items")


class UserFile(Base):
    __tablename__ = "user_files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String(1024))
    path: Mapped[str] = mapped_column(String(1024))

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="files")

    def __repr__(self) -> str:
        return f"UserFile(id={self.id}, filename={self.filename}, path={self.path})"


# endregion service data
# region anonymised data


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

    def encode(self) -> list[Any]:
        # точки сбыта
        return [
            self.dt,
            self.gtin,
            self.prid,
            self.inn,
            self.id_sp_,
            self.type_operation,
            self.price,
            self.cnt,
        ]


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


# endregion anonymised data

# region agregated data


class AgrProduction(Base):
    """Агрегированные данные о вводе товаров в оборот с 2021-11-22 по 2022-11-21."""

    __tablename__ = "agg_produced_goods"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    region_code: Mapped[int] = mapped_column(Integer)
    operation_type: Mapped[str] = mapped_column(String(27))
    org_count: Mapped[int] = mapped_column(Integer)
    assortment: Mapped[int] = mapped_column(Integer)
    count_brand: Mapped[int] = mapped_column(Integer)
    cnt: Mapped[int] = mapped_column(Integer)

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="agg_produced")

    def encode(self) -> list[Any]:
        return [
            self.dt,
            self.region_code,
            self.operation_type,
            self.org_count,
            self.assortment,
            self.count_brand,
            self.cnt,
        ]


class AgrSold(Base):
    """Агрегированные данные о выводе товаров из оборота с 2021-11-22 по 2022-11-21.csv"""

    __tablename__ = "agg_sold_goods"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    region_code: Mapped[int] = mapped_column(Integer)
    type_operation: Mapped[str] = mapped_column(String(512))
    count_active_point: Mapped[BigInteger] = mapped_column(BigInteger)
    org_count: Mapped[BigInteger] = mapped_column(BigInteger)
    assortment: Mapped[BigInteger] = mapped_column(BigInteger)
    count_brand: Mapped[BigInteger] = mapped_column(BigInteger)
    cnt: Mapped[BigInteger] = mapped_column(BigInteger)
    sum_price: Mapped[BigInteger] = mapped_column(BigInteger)

    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user: Mapped["User"] = relationship("User", back_populates="agr_sold")

    def encode(self) -> list[Any]:
        return [
            self.dt,
            self.sum_price,
            self.region_code,
        ]


class AgrTransported(Base):
    """Агрегированные данные о перемещениях товаров между участниками с 2021-11-22 по 2022-11-21"""

    __tablename__ = "agg_transported_goods"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    sender_region_code: Mapped[int] = mapped_column(Integer)
    receiver_region_code: Mapped[int] = mapped_column(Integer)
    sender_org_count: Mapped[int] = mapped_column(Integer)
    receiver_org_count: Mapped[int] = mapped_column(Integer)
    assortment: Mapped[int] = mapped_column(Integer)
    count_brand: Mapped[int] = mapped_column(Integer)
    cnt: Mapped[int] = mapped_column(Integer)


# endregion


# region additional data


class AddProducedGoods(Base):
    __tablename__ = "add_produced_goods"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    gtin: Mapped[str] = mapped_column(String(32))
    inn: Mapped[str] = mapped_column(String(64))
    product_name: Mapped[str] = mapped_column(String(64))
    product_short_name: Mapped[str] = mapped_column(String(64))
    tnved: Mapped[str] = mapped_column(String(64))
    tnved10: Mapped[str] = mapped_column(String(64))
    brand: Mapped[str] = mapped_column(String(64))
    country: Mapped[str] = mapped_column(String(256))
    volume: Mapped[str] = mapped_column(String(256))


class AddSoldGoods(Base):
    """Торговые точки"""

    __tablename__ = "add_sold_goods"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id_sp_: Mapped[str] = mapped_column(String(256))
    inn: Mapped[str] = mapped_column(String(64))
    region_code: Mapped[int] = mapped_column(Integer)
    city_with_type: Mapped[str] = mapped_column(String(256), nullable=True)
    city_fias_id: Mapped[str] = mapped_column(String(256), nullable=True)
    postal_code: Mapped[int] = mapped_column(Integer, nullable=True)


class OrganizationRegion(Base):
    """Данные о регионах пользователей"""

    __tablename__ = "organization_region"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    inn: Mapped[str] = mapped_column(String(64))
    region_code: Mapped[int] = mapped_column(Integer)


# endregion
