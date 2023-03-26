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
    popular_online_gtin_manufacturer,
    shops_manufacturer_count_region,
    shops_manufacturer_count,
    REGION_CODES,
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

    a = crud.get_agg_sold(db, user)
    dt, region_code, sum_price = zip(*a)
    data = {
        "dt": dt,
        "region_code": region_code,
        "sum_price": sum_price,
    }
    result = model.volume_agg_predict(data)
    return result


@router.get("/count_agg_predict")
async def predict_count(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
    user = get_current_user(db, token)
    model = Model()
    a = crud.get_agg_sold(db, user)
    dt, region_code, sum_price = zip(*a)
    data = {
        "dt": dt,
        "region_code": region_code,
        "sum_price": sum_price,
    }

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
    # check if shops_manufacturer not in REGION_CODES objects
    if any("shops_manufacturer" in d for d in REGION_CODES.values()):
        log.info(f"calculated {len(REGION_CODES)}  in {perf_counter() - start}")
        return REGION_CODES
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

    log.info(f"calculated {len(result)}  in {perf_counter() - start}")
    return result


@router.get("/volumes_manufacturer")
async def get_volume_metrics(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    """Количество единиц товара и стоимость всего проданного товара для 1 производителя в целом"""
    if any("volumes_manufacturer" in d for d in REGION_CODES.values()):
        log.info(f"calculated {len(REGION_CODES)}  in {perf_counter() - start}")
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
    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    additional_data = {
        "id_sp_": id_sp_,
        "region_code": region_code,
    }

    log.info(f"prepared data in {perf_counter() - start}")

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
    start = perf_counter()
    if any(
        "popular_offline_gtin_manufacturer_region" in d for d in REGION_CODES.values()
    ):
        log.info(f"calculated in {perf_counter() - start}")
        return REGION_CODES
    user = get_current_user(db, token)

    log.info("computing...")
    a = crud.get_sold_goods_for_offline_metrics(db, user)
    dt, id_sp_, gtin, type_operation, price, cnt = zip(*a)
    sold_data = {
        "dt": dt,
        "gtin": gtin,
        "id_sp_": id_sp_,
        "type_operation": type_operation,
        "price": price,
        "cnt": cnt,
    }

    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    additional_data = {
        "id_sp_": id_sp_,
        "region_code": region_code,
    }

    log.info(f"prepared data in {perf_counter() - start}")
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

    a = crud.get_sold_goods_for_offline_metrics(db, user)
    dt, id_sp_, gtin, type_operation, price, cnt = zip(*a)
    sold_data = {
        "dt": dt,
        "gtin": gtin,
        "id_sp_": id_sp_,
        "type_operation": type_operation,
        "price": price,
        "cnt": cnt,
    }

    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    additional_data = {"id_sp_": id_sp_, "region_code": region_code}
    #
    log.info("prepared data")

    result = popular_offline_gtin_manufacturer(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/popular_online_gtin_manufacturer")
async def get_popular_online_gtin_manufacturer(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    # rewrite sql query
    user = get_current_user(db, token)
    start = perf_counter()
    log.info("computing...")
    a = crud.get_sold_goods_for_online_metrics(db, user)
    dt, gtin, type_operation, cnt = zip(*a)
    sold_data = {
        "dt": dt,
        "gtin": gtin,
        "type_operation": type_operation,
        "cnt": cnt,
    }

    log.info(f"prepared data in {perf_counter() - start}")
    start = perf_counter()
    result = popular_online_gtin_manufacturer(sold_data)

    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/shops_manufacturer_count_region")
async def get_shops_manufacturer_count_region(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):

    user = get_current_user(db, token)
    start = perf_counter()
    a = crud.get_sold_goods_for_manufacturer_count_by_region(db, user)
    dt, id_sp_, cnt = zip(*a)
    sold_data = {
        "dt": dt,
        "id_sp_": id_sp_,
        "cnt": cnt,
    }

    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    additional_data = {"id_sp_": id_sp_, "region_code": region_code}
    log.info(f"prepared data in {perf_counter() - start}")
    start = perf_counter()
    result = shops_manufacturer_count_region(sold_data, additional_data)

    log.info(f"calculated in {perf_counter() - start}")
    return result


@router.get("/shops_manufacturer_count")
async def get_shops_manufacturer_count(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
):

    user = get_current_user(db, token)
    start = perf_counter()
    a = crud.get_sold_goods_for_manufacturer_count_by_region(db, user)
    dt, id_sp_, cnt = zip(*a)
    sold_data = {
        "dt": dt,
        "id_sp_": id_sp_,
        "cnt": cnt,
    }

    a = crud.get_points_for_mlcomputation(db)
    id_sp_, region_code = zip(*a)
    additional_data = {"id_sp_": id_sp_, "region_code": region_code}
    log.info(f"prepared data in {perf_counter() - start}")
    result = shops_manufacturer_count_region(sold_data, additional_data)
    log.info(f"calculated in {perf_counter() - start}")
    return result
