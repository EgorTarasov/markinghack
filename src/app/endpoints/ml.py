from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.dependencies import get_db
from app.core.auth import oauth2_scheme, get_current_user
from app.core import crud
from app.core import models
from app.core.ml import (
    Model,
    shops_manufacturer,
    volumes_manufacturer_region,
    volumes_manufacturer,
    popular_offline_gtin_manufacturer_region,
    popular_offline_gtin_manufacturer,
)

from app.utils.logging import log

from time import perf_counter

router = APIRouter(
    prefix="/ml",
    tags=["ml"],
    responses={404: {"description": "Not found"}},
)


# region Yarik ml


@router.get("/volume_agg_predict")
async def predict_volume(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
    user = get_current_user(db, token)

    model = Model()
    data = {
        "dt": [],
        "region_code": [],
        "sum_price": [],
    }

    for i in user.agr_sold:
        data["dt"].append(i.dt)
        data["region_code"].append(i.region_code)
        data["sum_price"].append(i.sum_price)
    result = model.volume_agg_predict(data)
    return result


@router.get("/count_agg_predict")
async def predict_count(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
    user = get_current_user(db, token)
    model = Model()
    data = {
        "dt": [],
        "region_code": [],
        "cnt": [],
    }

    for i in user.agr_sold:
        data["dt"].append(i.dt)
        data["region_code"].append(i.region_code)
        data["cnt"].append(i.sum_price)

    result = model.count_agg_predict(data)
    return result


@router.get("/volume_manufacturer_predict")
async def predict_manufacturer_volume(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    user = get_current_user(db, token)
    model = Model()
    data1 = {"dt": [], "cnt": [], "price": [], "id_sp_": []}

    # AddSoldGoods id_ps_ region_code

    for i in user.sold_goods:
        data1["dt"].append(i.dt)
        data1["cnt"].append(i.cnt)
        data1["price"].append(i.price)
        data1["id_sp_"].append(i.id_sp_)

    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    sale_points = {
        "id_sp_": id_sp_,
        "region_code": region_code,
    }
    result = model.volume_manufacturer_predict(data1, sale_points)
    return result


@router.get("/count_manufacturer_predict")
async def predict_manufacturer_count(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    user = get_current_user(db, token)
    model = Model()
    data1 = {"dt": [], "cnt": [], "price": [], "id_sp_": []}

    # AddSoldGoods id_ps_ region_code
    sale_points = {
        "id_sp_": [],
        "region_code": [],
    }
    for i in user.sold_goods:
        data1["dt"].append(i.dt)
        data1["cnt"].append(i.cnt)
        data1["price"].append(i.price)
        data1["id_sp_"].append(i.id_sp_)
    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    sale_points = {
        "id_sp_": id_sp_,
        "region_code": region_code,
    }

    result = model.count_manufacturer_predict(data1, sale_points)
    return result


# endregion Yarik ml


@router.get("/shops_manufacturer")
async def get_mertics(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
    user = get_current_user(db, token)
    start = perf_counter()
    log.info("computing...")

    start = perf_counter()
    a = crud.get_sold_goods_for_computation(db, user)
    dt, inn, id_sp_, type_operation = zip(*a)
    sold_data = {
        "dt": dt,
        "inn": inn,
        "id_sp_": id_sp_,
        "type_operation": type_operation,
    }

    a = crud.get_points_for_computation(db)
    id_sp_, region_code, city_with_type, postal_code = zip(*a)
    additional_data = {
        "id_sp_": id_sp_,
        "region_code": region_code,
        "city_with_type": city_with_type,
        "postal_code": postal_code,
    }
    log.info(f"prepared  data in {perf_counter() - start}")
    result = shops_manufacturer(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/volumes_manufacturer_region")
async def get_volume_metrics_by_region(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    # rewrite sql query
    user = get_current_user(db, token)
    start = perf_counter()
    log.info("computing...")

    sold_data = {
        "dt": [],
        "gtin": [],
        "prid": [],
        "inn": [],
        "id_sp_": [],
        "type_operation": [],
        "price": [],
        "cnt": [],
    }
    for i in user.sold_goods:
        sold_data["dt"].append(i.dt)
        sold_data["gtin"].append(i.gtin)
        sold_data["prid"].append(i.prid)
        sold_data["inn"].append(i.inn)
        sold_data["id_sp_"].append(i.id_sp_)
        sold_data["type_operation"].append(i.type_operation)
        sold_data["price"].append(i.price)
        sold_data["cnt"].append(i.cnt)

    additional_data = {
        "id_sp_": [],
        "inn": [],
        "region_code": [],
        "city_with_type": [],
        "city_fias_id": [],
        "postal_code": [],
    }
    points = get_points(db)
    for i in points:
        additional_data["id_sp_"].append(i.id_sp_)
        additional_data["inn"].append(i.inn)
        additional_data["region_code"].append(i.region_code)
        additional_data["city_with_type"].append(i.city_with_type)
        additional_data["city_fias_id"].append(i.city_fias_id)
        additional_data["postal_code"].append(i.postal_code)
    #
    log.info("prepared data")

    result = volumes_manufacturer_region(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/volumes_manufacturer")
async def get_volume_metrics(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    # rewrite sql query
    user = get_current_user(db, token)
    start = perf_counter()
    log.info("computing...")

    sold_data = {
        "dt": [],
        "gtin": [],
        "prid": [],
        "inn": [],
        "id_sp_": [],
        "type_operation": [],
        "price": [],
        "cnt": [],
    }
    for i in user.sold_goods:
        sold_data["dt"].append(i.dt)
        sold_data["gtin"].append(i.gtin)
        sold_data["prid"].append(i.prid)
        sold_data["inn"].append(i.inn)
        sold_data["id_sp_"].append(i.id_sp_)
        sold_data["type_operation"].append(i.type_operation)
        sold_data["price"].append(i.price)
        sold_data["cnt"].append(i.cnt)

    additional_data = {
        "id_sp_": [],
        "inn": [],
        "region_code": [],
        "city_with_type": [],
        "city_fias_id": [],
        "postal_code": [],
    }
    points = get_points(db)
    for i in points:
        additional_data["id_sp_"].append(i.id_sp_)
        additional_data["inn"].append(i.inn)
        additional_data["region_code"].append(i.region_code)
        additional_data["city_with_type"].append(i.city_with_type)
        additional_data["city_fias_id"].append(i.city_fias_id)
        additional_data["postal_code"].append(i.postal_code)
    #
    log.info("prepared data")

    result = volumes_manufacturer(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/popular_offline_gtin_manufacturer_region")
async def get_popular_offline_metrics_by_region(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    # rewrite sql query
    user = get_current_user(db, token)
    start = perf_counter()
    log.info("computing...")

    sold_data = {
        "dt": [],
        "gtin": [],
        "prid": [],
        "inn": [],
        "id_sp_": [],
        "type_operation": [],
        "price": [],
        "cnt": [],
    }
    for i in user.sold_goods:
        sold_data["dt"].append(i.dt)
        sold_data["gtin"].append(i.gtin)
        sold_data["prid"].append(i.prid)
        sold_data["inn"].append(i.inn)
        sold_data["id_sp_"].append(i.id_sp_)
        sold_data["type_operation"].append(i.type_operation)
        sold_data["price"].append(i.price)
        sold_data["cnt"].append(i.cnt)

    additional_data = {
        "id_sp_": [],
        "inn": [],
        "region_code": [],
        "city_with_type": [],
        "city_fias_id": [],
        "postal_code": [],
    }
    points = get_points(db)
    for i in points:
        additional_data["id_sp_"].append(i.id_sp_)
        additional_data["inn"].append(i.inn)
        additional_data["region_code"].append(i.region_code)
        additional_data["city_with_type"].append(i.city_with_type)
        additional_data["city_fias_id"].append(i.city_fias_id)
        additional_data["postal_code"].append(i.postal_code)
    #
    log.info("prepared data")

    result = popular_offline_gtin_manufacturer_region(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/popular_offline_gtin_manufacturer")
async def get_popular_offline_metrics(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    # rewrite sql query
    user = get_current_user(db, token)
    start = perf_counter()

    log.info("computing...")

    sold_data = {
        "dt": [],
        "gtin": [],
        "prid": [],
        "inn": [],
        "id_sp_": [],
        "type_operation": [],
        "price": [],
        "cnt": [],
    }
    for i in user.sold_goods:
        sold_data["dt"].append(i.dt)
        sold_data["gtin"].append(i.gtin)
        sold_data["prid"].append(i.prid)
        sold_data["inn"].append(i.inn)
        sold_data["id_sp_"].append(i.id_sp_)
        sold_data["type_operation"].append(i.type_operation)
        sold_data["price"].append(i.price)
        sold_data["cnt"].append(i.cnt)

    additional_data = {
        "id_sp_": [],
        "inn": [],
        "region_code": [],
        "city_with_type": [],
        "city_fias_id": [],
        "postal_code": [],
    }
    points = get_points(db)
    for i in points:
        additional_data["id_sp_"].append(i.id_sp_)
        additional_data["inn"].append(i.inn)
        additional_data["region_code"].append(i.region_code)
        additional_data["city_with_type"].append(i.city_with_type)
        additional_data["city_fias_id"].append(i.city_fias_id)
        additional_data["postal_code"].append(i.postal_code)
    #
    log.info("prepared data")

    result = popular_offline_gtin_manufacturer(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return result


# @router.post("/create_user", response_model=UserSchema)
# async def create_user(
#     user: UserCreateSchema, db: Session = Depends(get_db)
# ) -> UserSchema:
#     user.password = get_password_hash(user.password)
#     user = save_user(db, user.username, user.password, user.email)
#     return UserSchema.from_orm(user)


# @router.get("/me", response_model=UserSchema)
# async def profile(
#     token=Depends(oauth2_scheme), db: Session = Depends(get_db)
# ) -> UserSchema:
#     user = get_current_user(db, token)
#     return UserSchema.from_orm(user)
