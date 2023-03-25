from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.dependencies import get_db
from app.core.auth import oauth2_scheme, get_current_user
from app.core.crud import get_points
from app.core.ml import Model, shops_manufacturer

from app.utils.logging import log

from time import perf_counter

router = APIRouter(
    prefix="/ml",
    tags=["ml"],
    responses={404: {"description": "Not found"}},
)


@router.get("/predict")
async def predict(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
    user = get_current_user(db, token)
    # model = Model()
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
    result = model.predict(data)
    return result


@router.get("/shops_manufacturer")
async def get_mertics(token=Depends(oauth2_scheme), db: Session = Depends(get_db)):
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

    result = shops_manufacturer(sold_data, additional_data)
    log.info(len(result))
    print(result)
    log.info(f"calculated in {perf_counter() - start}")
    return {"status": "ok"}


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