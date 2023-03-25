from typing import Union
from datetime import datetime

from pydantic import BaseModel, PrivateAttr, Field

from app.core.models import User, Item, ProducedGoods, SoldGoods, TransportedGoods

# region User
class UserSchema(BaseModel):
    __id__: int = PrivateAttr(...)
    username: str = Field(...)
    _password: str = PrivateAttr(...)
    email: str = Field(...)

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "username": "Alandez",
                "email": "test@test.com",
            }
        }


class UserCreateSchema(BaseModel):
    username: str = Field(...)
    password: str = Field(...)
    email: str = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "username": "Alandez",
                "password": "SuperStr0ngPassw0rd",
                "email": "test@test.com",
            }
        }


# endregion User

# region Token
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


# endregion Token

# region Goods


class ProducedGoodsSchema(BaseModel):
    __id__: int = PrivateAttr(...)
    dt: datetime = Field(...)
    gtin: str = Field(...)
    prid: str = Field(...)
    operation_type: str = Field(...)
    cnt: int = Field(...)

    class Config:
        orm_mode = True


class ListProducedGoodsSchema(BaseModel):
    items: list[ProducedGoodsSchema]


class SoldGoodsSchema(BaseModel):
    __id__: int = PrivateAttr(...)
    dt: datetime = Field(...)
    gtin: str = Field(...)
    prid: str = Field(...)
    inn: str = Field(...)
    id_sp_: str = Field(...)
    type_operation: str = Field(...)
    price: int = Field(...)
    cnt: int = Field(...)

    class Config:
        orm_mode = True


class ListSoldGoodsSchema(BaseModel):
    items: list[SoldGoodsSchema]


class TransportedGoodsSchema(BaseModel):
    __id__: int = PrivateAttr(...)
    dt: datetime = Field(...)
    gtin: str = Field(...)
    prid: str = Field(...)
    inn: str = Field(...)
    sender_inn: str = Field(...)
    receiver_inn: str = Field(...)
    cnt_moved: int = Field(...)

    class Config:
        orm_mode = True


class ListTransportedGoodsSchema(BaseModel):
    items: list[TransportedGoodsSchema]


class AgrProducedGoodsSchema(BaseModel):
    __id__: int = PrivateAttr(...)


# endregion Goods


# region mapPoint


class MapPoint(BaseModel):
    short: str = Field(...)
    name: str = Field(...)
    code: int = Field(...)
    value: float = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "short": "Мо",
                "name": "Москва",
                "code": 77,
                "value": 100.0,
            }
        }


# end   region mapPoint
