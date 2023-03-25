from typing import Optional

from sqlalchemy.orm import Session

from app.core import models
from app.core import schemas

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


def get_user_by_username(db: Session, username: str) -> models.User | None:
    return db.query(models.User).filter(models.User.username == username).one_or_none()


# endregion user
# region item


def save_item(db: Session, name: str, user: models.User) -> models.Item:
    db_item = models.Item(name=name, user=user)
    db.add(db_item)
    db.commit()
    return db_item


def get_users_items(db: Session, user: models.User) -> list[models.Item] | None:
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


# endregion sold goods

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


def get_points(db: Session):
    return db.query(models.AddSoldGoods).all()


# endregion additional data
