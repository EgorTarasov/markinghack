from datetime import datetime
from time import perf_counter

from app.core.database import *
from app.core.models import *
import os


files = {
    "prod": "data/addon/production.csv",
    "sold": "data/addon/sold.csv",
    "transport": "data/addon/transport.csv",
}

SessionLocal = init_db()
db = SessionLocal()


with open(files["prod"], "r") as file:
    fields = file.readline()
    # "dt","inn","gtin","prid","operation_type","cnt"
    goods = []
    start = perf_counter()
    for l in file.readlines():
        print(l.split(","))
        goods.append(
            ProducedGoods(
                dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
                inn=str(l.split(",")[1]).replace('"', ""),
                gtin=str(l.split(",")[2]).replace('"', ""),
                prid=str(l.split(",")[3]).replace('"', ""),
                operation_type=str(l.split(",")[4]).replace('"', ""),
                cnt=int(l.split(",")[5]),
            )
        )
    db.add_all(goods)
    db.commit()
    print(perf_counter() - start)

with open(files["sold"], "r") as file:
    fields = file.readline()
    # "dt","gtin","prid","inn","id_sp_","type_operation","price","cnt"
    goods = []
    start = perf_counter()
    for l in file.readlines():
        goods.append(
            SoldGoods(
                dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
                gtin=str(l.split(",")[1]).replace('"', ""),
                prid=str(l.split(",")[2]).replace('"', ""),
                inn=str(l.split(",")[3]).replace('"', ""),
                id_sp_=str(l.split(",")[4]).replace('"', ""),
                type_operation=str(l.split(",")[5]).replace('"', ""),
                price=int(l.split(",")[6]),
                cnt=int(l.split(",")[7]),
            )
        )
    db.add_all(goods)
    db.commit()
    print(perf_counter() - start)

with open(files["transport"], "r") as file:
    fields = file.readline()
    print(fields)
    goods = []
    start = perf_counter()
    for l in file.readlines():
        goods.append(
            TransportedGoods(
                dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
                gtin=str(l.split(",")[1]).replace('"', ""),
                prid=str(l.split(",")[2]).replace('"', ""),
                sender_inn=str(l.split(",")[3]).replace('"', ""),
                receiver_inn=str(l.split(",")[4]).replace('"', ""),
                cnt_moved=int(l.split(",")[5]),
            )
        )

    db.add_all(goods)
    db.commit()
    print(perf_counter() - start)
