from typing import Annotated
from time import perf_counter

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
from app.core import crud
from app.core.ml import volumes_manufacturer_region


router = APIRouter(prefix="/data", tags=["data"])


router = APIRouter(prefix="/map", tags=["map"])


@router.get("/get")
async def get_map(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
    user = get_current_user(db, token)
    start = perf_counter()
    log.info("computing...")

    a = crud.get_sold_goods_volume_metrics_by_region(db, user)
    dt, id_sp_, cnt, price = zip(*a)
    sold_data = {
        "dt": dt,
        "id_sp_": id_sp_,
        "price": cnt,
        "cnt": price,
    }
    a = crud.get_points_for_computation(db)
    id_sp_, region_code, city_with_type, _ = zip(*a)
    additional_data = {
        "id_sp_": id_sp_,
        "region_code": region_code,
        "city_with_type": city_with_type,
    }
    log.info("preparing data took %s seconds", perf_counter() - start)
    start = perf_counter()

    result = volumes_manufacturer_region(sold_data, additional_data)
    log.info("computation took %s seconds", perf_counter() - start)

    return result.values()
