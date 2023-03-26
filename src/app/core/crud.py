from typing import Optional, Union
from time import perf_counter

from sqlalchemy.orm import Session

from app.core import models
from app.core import schemas
from app.utils.logging import log

# region user
def save_user(
    db: Session,
    username: str,
    hashed_password: str,
    email: str,
    fullname: Optional[str] = None,
) -> models.User:

    db_user = models.User(
        username=username,
        password=hashed_password,
        email=email,
    )
    if fullname:
        db_user.fullname = fullname
    db.add(db_user)
    db.commit()
    return db_user


def get_user_by_username(db: Session, username: str) -> Union[models.User, None]:
    return db.query(models.User).filter(models.User.username == username).one_or_none()


# endregion user
# region item


def save_item(db: Session, name: str, user: models.User) -> models.Item:
    db_item = models.Item(name=name, user=user)
    db.add(db_item)
    db.commit()
    return db_item


def get_users_items(db: Session, user: models.User) -> Union[list[models.Item], None]:
    items = user.items
    return items


# endregion item

# region produced goods
def get_produced_goods(
    db: Session, offset: int = 0, count: int = 100
) -> list[models.ProducedGoods]:
    return db.query(models.ProducedGoods).offset(offset).limit(count).all()


# endregion produced goods


# region sold goods
def get_sold_goods(
    db: Session, offset: int = 0, count: int = 100
) -> list[models.SoldGoods]:
    return db.query(models.SoldGoods).offset(offset).limit(count).all()


def get_sold_goods_for_computation(db: Session, user: models.User):

    # result = SomeModel.query.with_entities(SomeModel.col1, SomeModel.col2)
    start = perf_counter()
    result = (
        db.query(models.SoldGoods)
        .with_entities(
            models.SoldGoods.dt,
            models.SoldGoods.inn,
            models.SoldGoods.id_sp_,
            models.SoldGoods.type_operation,
        )
        .filter(models.SoldGoods.user_id == user.id)
        .all()
    )
    log.info(
        f"get_sold_goods_for_computation ({len(result)}): {perf_counter() - start}"
    )
    return result


def get_sold_goods_volume_metrics_by_region(db: Session, user: models.User):
    start = perf_counter()
    result = (
        db.query(models.SoldGoods)
        .with_entities(
            models.SoldGoods.dt,
            models.SoldGoods.id_sp_,
            models.SoldGoods.price,
            models.SoldGoods.cnt,
        )
        .filter(models.SoldGoods.user_id == user.id)
        .all()
    )
    return result


def get_sold_goods_for_offline_metrics(db: Session, user: models.User):
    # "dt", "id_sp_", "gtin", "cnt", "price", "type_operation"
    result = (
        db.query(models.SoldGoods)
        .with_entities(
            models.SoldGoods.dt,
            models.SoldGoods.id_sp_,
            models.SoldGoods.gtin,
            models.SoldGoods.type_operation,
            models.SoldGoods.price,
            models.SoldGoods.cnt,
        )
        .filter(models.SoldGoods.user_id == user.id)
        .all()
    )
    return result


def get_sold_goods_for_online_metrics(db: Session, user: models.User):
    result = (
        db.query(models.SoldGoods)
        .with_entities(
            models.SoldGoods.dt,
            models.SoldGoods.gtin,
            models.SoldGoods.type_operation,
            models.SoldGoods.cnt,
        )
        .filter(models.SoldGoods.user_id == user.id)
        .all()
    )
    return result


def get_sold_goods_for_manufacturer_count_by_region(db: Session, user: models.User):
    result = (
        db.query(models.SoldGoods)
        .with_entities(
            models.SoldGoods.dt,
            models.SoldGoods.id_sp_,
            models.SoldGoods.cnt,
        )
        .filter(models.SoldGoods.user_id == user.id)
        .all()
    )
    return result


# endregion sold goods

# region agg data
def get_agg_sold(db: Session, user: models.User):
    return (
        db.query(models.AgrSold)
        .with_entities(
            models.AgrSold.dt, models.AgrSold.region_code, models.AgrSold.sum_price
        )
        .filter(models.AgrSold.user_id == user.id)
        .all()
    )


# endregion


# region user files
def save_file(
    db: Session, user: models.User, filename: str, filepath
) -> models.UserFile:
    db_file = models.UserFile(
        filename=filename,
        path=filepath,
        user=user,
    )
    db.add(db_file)
    db.commit()
    return db_file


# endregion user files

# region additional data


def get_region_by_inn(db: Session, inn: str) -> int:
    data = (
        db.query(models.OrganizationRegion)
        .filter(models.OrganizationRegion.inn == inn)
        .one_or_none()
    )
    if data:
        return data.region_code
    return -1


def get_points_for_computation(
    db: Session,
):

    result = (
        db.query(models.AddSoldGoods)
        .with_entities(
            models.AddSoldGoods.id_sp_,
            models.AddSoldGoods.region_code,
            models.AddSoldGoods.city_with_type,
            models.AddSoldGoods.postal_code,
        )
        .all()
    )

    return result


def get_points_for_mlcomputation(
    db: Session,
):

    result = (
        db.query(models.AddSoldGoods)
        .with_entities(
            models.AddSoldGoods.id_sp_,
            models.AddSoldGoods.region_code,
        )
        .all()
    )

    return result


# endregion additional data
