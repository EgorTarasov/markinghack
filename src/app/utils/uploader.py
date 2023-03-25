import os
from datetime import datetime
from time import perf_counter
from sqlalchemy.orm import Session

from app.core.models import User, ProducedGoods, SoldGoods, TransportedGoods, UserFile
from app.utils.logging import log


def upload_from_csv(db: Session, file: UserFile):
    log.debug(file)
    with open(file.path, "r") as csvfile:
        fields = csvfile.readline()
        log.info(fields, type(fields))
        # "dt","inn","gtin","prid","operation_type","cnt"
        goods = []
        start = perf_counter()
        if fields == '"dt","inn","gtin","prid","operation_type","cnt"\n':

            for l in csvfile.readlines():
                goods.append(
                    ProducedGoods(
                        dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
                        inn=str(l.split(",")[1]).replace('"', ""),
                        gtin=str(l.split(",")[2]).replace('"', ""),
                        prid=str(l.split(",")[3]).replace('"', ""),
                        operation_type=str(l.split(",")[4]).replace('"', ""),
                        cnt=int(l.split(",")[5]),
                        user=file.user,
                    )
                )

        elif (
            fields
            == '"dt","gtin","prid","inn","id_sp_","type_operation","price","cnt"\n'
        ):
            # "dt","gtin","prid","inn","id_sp_","type_operation","price","cnt"
            for l in csvfile.readlines():
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
                        user=file.user,
                    )
                )
        elif fields == '"dt","gtin","prid","sender_inn","receiver_inn","cnt_moved"\n':
            for l in csvfile.readlines():
                goods.append(
                    TransportedGoods(
                        dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
                        gtin=str(l.split(",")[1]).replace('"', ""),
                        prid=str(l.split(",")[2]).replace('"', ""),
                        sender_inn=str(l.split(",")[3]).replace('"', ""),
                        receiver_inn=str(l.split(",")[4]).replace('"', ""),
                        cnt_moved=int(l.split(",")[5]),
                        user=file.user,
                    )
                )
        db.add_all(goods)
        db.commit()
        log.info(f"Uploaded {len(goods)} goods in {perf_counter() - start} seconds")
