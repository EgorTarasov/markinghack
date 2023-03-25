from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Query,
    Path,
    UploadFile,
    File,
    BackgroundTasks,
)
from sqlalchemy.orm import Session

from app.core.dependencies import get_db
from app.core.auth import get_password_hash, oauth2_scheme, get_current_user
from app.core.crud import get_produced_goods, get_sold_goods, save_file
from app.core.schemas import *
from app.core.models import ProducedGoods, SoldGoods, TransportedGoods
from app.core.settings import settings
from app.utils.logging import log
from app.utils.uploader import upload_from_csv


router = APIRouter(prefix="/goods", tags=["goods"])

# region produced goods
@router.get("/produced", response_model=ListProducedGoodsSchema)
async def get(
    offset: Annotated[
        int,
        Query(ge=0),
    ],
    count: Annotated[
        int,
        Query(
            title="смещение, необходимое для выборки определенного подмножества товаров. положительное число. по умолчанию 0.",
            ge=1,
            le=100,
        ),
    ],
    token=Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    """Возвращает список произведенных товаров
    | Параметр     | Описание                                                                                                   |
    |--------------|------------------------------------------------------------------------------------------------------------|
    | offset (int) | смещение, необходимое для выборки определенного подмножества товаров. положительное число. по умолчанию 0. |
    | count (int)  | количество  товаров, которое необходимо получить. Максимальное значение: 100                               |
    | filter (str) | Определяет, какие типы товаров необходимо получить Возможные значения:
    """
    user = get_current_user(db, token)

    if user:
        return ListProducedGoodsSchema(
            items=[
                ProducedGoodsSchema.from_orm(good)
                for good in get_produced_goods(db, offset, count)
            ]
        )
    else:
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


# @router.post("/")


# endregion produced goods


# region sold goods
@router.get(
    "/sold", response_model=ListSoldGoodsSchema
)  # , response_model=list[SoldGoods])
async def get_sold(
    offset: Annotated[
        int,
        Query(
            ge=0,
        ),
    ],
    count: Annotated[
        int,
        Query(
            ge=1,
            le=100,
        ),
    ],
    token=Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    user = get_current_user(db, token)
    if user:
        return ListSoldGoodsSchema(
            items=[
                SoldGoodsSchema.from_orm(good)
                for good in get_sold_goods(db, offset, count)
            ]
        )
    else:
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


# endregion sold goods


# region files


@router.post("/upload")
def upload(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    token=Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    user = get_current_user(db, token)
    try:
        contents = file.file.read()
        file_path = settings.STATIC_FILE_URL.format(
            username=user.id, filename=file.filename
        )
        file_name = file.filename
        with open(
            file_path,
            "wb",
        ) as f:
            f.write(contents)
        db_file = save_file(db, user, file_name, file_path) # type: ignore
        background_tasks.add_task(upload_from_csv, db, db_file)
    except Exception as ex:
        log.exception(f"Error uploading file {ex}")
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()

    return {"message": f"Successfully uploaded {file.filename}"}


# endregion files
