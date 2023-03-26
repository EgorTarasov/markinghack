import os
import csv
from datetime import datetime
from time import perf_counter
from sqlalchemy.orm import Session
import multiprocessing as mp

from app.core.models import (
    User,
    ProducedGoods,
    SoldGoods,
    TransportedGoods,
    UserFile,
    AgrProduction,
    AgrSold,
    AgrTransported,
    AddSoldGoods,
)
from app.utils.logging import log


def upload_from_csv(db: Session, file: UserFile):
    log.info(file)
    with open(file.path, "r") as csvfile:
        start = perf_counter()
        csv_reader = csv.reader(csvfile, delimiter=",")
        goods = []
        fields = []

        model_type = None
        for i, row in enumerate(csv_reader):
            if i % 100 == 0:
                log.info(i)
            if not i:
                log.info(row)
                if (
                    row
                    == "['dt', 'region_code', 'operation_type', 'org_count', 'assortment', 'count_brand', 'cnt']"
                ):
                    model_type = 4
                elif row == [
                    "dt",
                    "region_code",
                    "type_operation",
                    "count_active_point",
                    "org_count",
                    "assortment",
                    "count_brand",
                    "cnt",
                    "sum_price",
                ]:
                    model_type = 5
                elif row == [
                    "t1.dt",
                    "sender_region_code",
                    "receiver_region_code",
                    "sender_org_count",
                    "receiver_org_count",
                    "assortment",
                    "count_brand",
                    "cnt_moved",
                ]:
                    model_type = 6
                elif row == ["dt", "inn", "gtin", "prid", "operation_type", "cnt"]:
                    model_type = 1
                elif row == [
                    "dt",
                    "gtin",
                    "prid",
                    "inn",
                    "id_sp_",
                    "type_operation",
                    "price",
                    "cnt",
                ]:
                    model_type = 2
                elif row == [
                    "dt",
                    "gtin",
                    "prid",
                    "sender_inn",
                    "receiver_inn",
                    "cnt_moved",
                ]:
                    model_type = 3
                elif row == [
                    "id_sp_",
                    "inn",
                    "region_code",
                    "city_with_type",
                    "city_fias_id",
                    "postal_code",
                ]:
                    model_type = 7
                log.info(model_type)
                log.info(row)
                log.info(
                    row
                    == [
                        "dt",
                        "gtin",
                        "prid",
                        "inn",
                        "id_sp_",
                        "type_operation",
                        "price",
                        "cnt",
                    ]
                )
                continue
            if model_type == 5:
                goods.append(
                    AgrSold(
                        dt=datetime.strptime(row[0], "%Y-%m-%d"),
                        region_code=str(row[1]),
                        type_operation=str(row[2]),
                        count_active_point=int(row[3]),
                        org_count=int(row[4]),
                        assortment=int(row[5]),
                        count_brand=int(row[6]),
                        cnt=int(row[7]),
                        sum_price=int(row[8]),
                        user=file.user,
                        user_id=file.user.id,
                    )
                )
            elif model_type == 4:
                goods.append(
                    AgrProduction(
                        dt=datetime.strptime(row[0], "%Y-%m-%d"),
                        sender_region_code=int(row[1]),
                        receiver_region_code=int(row[2]),
                        assortment=int(row[3]),
                        count_brand=int(row[4]),
                        cnt=int(row[5]),
                        user=file.user,
                        user_id=file.user.id,
                    )
                )
            elif model_type == 6:
                goods.append(
                    AgrTransported(
                        dt=datetime.strptime(row[0], "%Y-%m-%d"),
                        sender_region_code=int(row[1]),
                        receiver_region_code=int(row[2]),
                        sender_org_count=int(row[3]),
                        receiver_org_count=int(row[4]),
                        assortment=int(row[5]),
                        count_brand=int(row[6]),
                        cnt_moved=int(row[7]),
                        user=file.user,
                        user_id=file.user.id,
                    )
                )
            elif model_type == 1:
                goods.append(
                    ProducedGoods(
                        dt=datetime.strptime(row[0], "%Y-%m-%d"),
                        inn=str(row[1]),
                        gtin=str(row[2]),
                        prid=str(row[3]),
                        operation_type=str(row[4]),
                        cnt=int(row[5]),
                        user_id=file.user.id,
                        user=file.user,
                    )
                )
            elif model_type == 2:
                goods.append(
                    SoldGoods(
                        dt=datetime.strptime(row[0], "%Y-%m-%d"),
                        gtin=str(row[1]),
                        prid=str(row[2]),
                        inn=str(row[3]),
                        id_sp_=str(row[4]),
                        type_operation=str(row[5]),
                        price=int(row[6]),
                        cnt=int(row[7]),
                        user_id=file.user.id,
                        user=file.user,
                    )
                )

            elif model_type == 3:
                goods.append(
                    TransportedGoods(
                        dt=datetime.strptime(row[0], "%Y-%m-%d"),
                        gtin=str(row[1]),
                        prid=str(row[2]),
                        sender_inn=str(row[3]),
                        receiver_inn=str(row[4]),
                        cnt_moved=int(row[5]),
                        user_id=file.user.id,
                        user=file.user,
                    )
                )
            elif model_type == 7:
                city_with_type, city_fias_id, posttal_code = row[3], row[4], row[5]
                if not city_with_type:
                    city_with_type = None
                if not city_fias_id:
                    city_fias_id = None
                if not posttal_code:
                    posttal_code = None
                goods.append(
                    AddSoldGoods(
                        id_sp_=str(row[0]),
                        inn=str(row[1]),
                        region_code=int(row[2]),
                        city_with_type=city_with_type,
                        city_fias_id=city_fias_id,
                        postal_code=posttal_code,
                    )
                )
        db.bulk_save_objects(goods)
        db.commit()
        log.info(f"Uploaded {len(goods)} goods in {perf_counter() - start} seconds")


# # process file function
# def processfile(filename, start=0, stop=0):
#     if start == 0 and stop == 0:
#         ... process entire file...
#     else:
#         with open(file, 'r') as fh:
#             fh.seek(start)
#             lines = fh.readlines(stop - start)
#             for i, row in lines:
#                 if i % 100 == 0:
#                     log.info(i)
#                 if i  == 0:
#                     log.info(row)
#                     if (
#                         row
#                         == "['dt', 'region_code', 'operation_type', 'org_count', 'assortment', 'count_brand', 'cnt']"
#                     ):
#                         model_type = 4
#                     elif row == [
#                         "dt",
#                         "region_code",
#                         "type_operation",
#                         "count_active_point",
#                         "org_count",
#                         "assortment",
#                         "count_brand",
#                         "cnt",
#                         "sum_price",
#                     ]:
#                         model_type = 5
#                     elif row == [
#                         "t1.dt",
#                         "sender_region_code",
#                         "receiver_region_code",
#                         "sender_org_count",
#                         "receiver_org_count",
#                         "assortment",
#                         "count_brand",
#                         "cnt_moved",
#                     ]:
#                         model_type = 6
#                     elif row == ["dt", "inn", "gtin", "prid", "operation_type", "cnt"]:
#                         model_type = 1
#                     elif row == [
#                         "dt",
#                         "gtin",
#                         "prid",
#                         "inn",
#                         "id_sp_",
#                         "type_operation",
#                         "price",
#                         "cnt",
#                     ]:
#                         model_type = 2
#                     elif row == [
#                         "dt",
#                         "gtin",
#                         "prid",
#                         "sender_inn",
#                         "receiver_inn",
#                         "cnt_moved",
#                     ]:
#                         model_type = 3
#                     elif row == [
#                         "id_sp_",
#                         "inn",
#                         "region_code",
#                         "city_with_type",
#                         "city_fias_id",
#                         "postal_code",
#                     ]:
#                         model_type = 7
#                     log.info(model_type)
#                     log.info(row)
#                     log.info(
#                         row
#                         == [
#                             "dt",
#                             "gtin",
#                             "prid",
#                             "inn",
#                             "id_sp_",
#                             "type_operation",
#                             "price",
#                             "cnt",
#                         ]
#                     )
#                     continue
#                 elif model_type == 5:
#                     goods.append(
#                         AgrSold(
#                             dt=datetime.strptime(row[0], "%Y-%m-%d"),
#                             region_code=str(row[1]),
#                             type_operation=str(row[2]),
#                             count_active_point=int(row[3]),
#                             org_count=int(row[4]),
#                             assortment=int(row[5]),
#                             count_brand=int(row[6]),
#                             cnt=int(row[7]),
#                             sum_price=int(row[8]),
#                             user=file.user,
#                             user_id=file.user.id,
#                         )
#                     )
#                 elif model_type == 4:
#                     goods.append(
#                         AgrProduction(
#                             dt=datetime.strptime(row[0], "%Y-%m-%d"),
#                             sender_region_code=int(row[1]),
#                             receiver_region_code=int(row[2]),
#                             assortment=int(row[3]),
#                             count_brand=int(row[4]),
#                             cnt=int(row[5]),
#                             user=file.user,
#                             user_id=file.user.id,
#                         )
#                     )
#                 elif model_type == 6:
#                     goods.append(
#                         AgrTransported(
#                             dt=datetime.strptime(row[0], "%Y-%m-%d"),
#                             sender_region_code=int(row[1]),
#                             receiver_region_code=int(row[2]),
#                             sender_org_count=int(row[3]),
#                             receiver_org_count=int(row[4]),
#                             assortment=int(row[5]),
#                             count_brand=int(row[6]),
#                             cnt_moved=int(row[7]),
#                             user=file.user,
#                             user_id=file.user.id,
#                         )
#                     )
#                 elif model_type == 1:
#                     goods.append(
#                         ProducedGoods(
#                             dt=datetime.strptime(row[0], "%Y-%m-%d"),
#                             inn=str(row[1]),
#                             gtin=str(row[2]),
#                             prid=str(row[3]),
#                             operation_type=str(row[4]),
#                             cnt=int(row[5]),
#                             user_id=file.user.id,
#                             user=file.user,
#                         )
#                     )
#                 elif model_type == 2:
#                     goods.append(
#                         SoldGoods(
#                             dt=datetime.strptime(row[0], "%Y-%m-%d"),
#                             gtin=str(row[1]),
#                             prid=str(row[2]),
#                             inn=str(row[3]),
#                             id_sp_=str(row[4]),
#                             type_operation=str(row[5]),
#                             price=int(row[6]),
#                             cnt=int(row[7]),
#                             user_id=file.user.id,
#                             user=file.user,
#                         )
#                     )

#                 elif model_type == 3:
#                     goods.append(
#                         TransportedGoods(
#                             dt=datetime.strptime(row[0], "%Y-%m-%d"),
#                             gtin=str(row[1]),
#                             prid=str(row[2]),
#                             sender_inn=str(row[3]),
#                             receiver_inn=str(row[4]),
#                             cnt_moved=int(row[5]),
#                             user_id=file.user.id,
#                             user=file.user,
#                         )
#                     )
#                 elif model_type == 7:
#                     city_with_type, city_fias_id, posttal_code = row[3], row[4], row[5]
#                     if not city_with_type:
#                         city_with_type = None
#                     if not city_fias_id:
#                         city_fias_id = None
#                     if not posttal_code:
#                         posttal_code = None
#                     goods.append(
#                         AddSoldGoods(
#                             id_sp_=str(row[0]),
#                             inn=str(row[1]),
#                             region_code=int(row[2]),
#                             city_with_type=city_with_type,
#                             city_fias_id=city_fias_id,
#                             postal_code=posttal_code,
#                         )
#                     )
#             db.bulk_save_objects(goods)
#             db.commit()
#             log.info(f"Uploaded {len(goods)} goods in {perf_counter() - start} seconds")


#     return results


# def parce(db: Session, file: UserFile):
#         # get file size and set chuck size
#     filesize = os.path.getsize(file.path)
#     split_size = 100*1024*1024

#     # determine if it needs to be split
#     if filesize > split_size:

#         # create pool, initialize chunk start location (cursor)
#         pool = mp.Pool(4)
#         cursor = 0
#         results = []
#         with open(file, 'r') as fh:

#             # for every chunk in the file...
#             for chunk in range(filesize // split_size):

#                 # determine where the chunk ends, is it the last one?
#                 if cursor + split_size > filesize:
#                     end = filesize
#                 else:
#                     end = cursor + split_size

#                 # seek to end of chunk and read next line to ensure you
#                 # pass entire lines to the processfile function
#                 fh.seek(end)
#                 fh.readline()

#                 # get current file location
#                 end = fh.tell()

#                 # add chunk to process pool, save reference to get results
#                 proc = pool.apply_async(processfile, args=[file.path, cursor, end])
#                 results.append(proc)

#                 # setup next chunk
#                 cursor = end

#         # close and wait for pool to finish
#         pool.close()
#         pool.join()

#         # iterate through results
#         for proc in results:
#             processfile_result = proc.get()


# # fields = csvfile.readline()
# # log.info(fields)
# # # "dt","inn","gtin","prid","operation_type","cnt"
# # goods = []
# # start = perf_counter()
# # if (
# #     fields
# #     == '"dt","region_code","type_operation","count_active_point","org_count","assortment","count_brand","cnt","sum_price"\n'
# # ):
# #     for l in csvfile.readlines():
# #         goods.append(
# #             AgrSold(
# #                 dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
# #                 region_code=str(l.split(",")[1]).replace('"', ""),
# #                 type_operation=str(l.split(",")[2]).replace('"', ""),
# #                 count_active_point=int(l.split(",")[3].replace('"', "")),
# #                 org_count=int(l.split(",")[4].replace('"', "")),
# #                 assortment=int(l.split(",")[5].replace('"', "")),
# #                 count_brand=int(l.split(",")[6].replace('"', "")),
# #                 cnt=int(l.split(",")[7].replace('"', "")),
# #                 sum_price=int(l.split(",")[8].replace('"', "")),
# #                 user=file.user,
# #             )
# #         )
# # elif (
# #     fields
# #     == '"dt","region_code","operation_type","org_count","assortment","count_brand","cnt"'
# # ):
# #     for l in csvfile.readlines():
# #         goods.append(
# #             AgrProduction(
# #                 dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
# #                 region_code=str(l.split(",")[1]).replace('"', ""),
# #                 operation_type=str(l.split(",")[2]).replace('"', ""),
# #                 org_count=int(l.split(",")[3].replace('"', "")),
# #                 assortment=int(l.split(",")[4].replace('"', "")),
# #                 count_brand=int(l.split(",")[5].replace('"', "")),
# #                 cnt=int(l.split(",")[6].replace('"', "")),
# #                 user=file.user,
# #             )
# #         )

# # elif fields == '"dt","inn","gtin","prid","operation_type","cnt"\n':

# #     for l in csvfile.readlines():
# #         goods.append(
# #             ProducedGoods(
# #                 dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
# #                 inn=str(l.split(",")[1]).replace('"', ""),
# #                 gtin=str(l.split(",")[2]).replace('"', ""),
# #                 prid=str(l.split(",")[3]).replace('"', ""),
# #                 operation_type=str(l.split(",")[4]).replace('"', ""),
# #                 cnt=int(l.split(",")[5]),
# #                 user=file.user,
# #             )
# #         )

# # elif (
# #     fields
# #     == '"dt","gtin","prid","inn","id_sp_","type_operation","price","cnt"\n'
# # ):
# #     # "dt","gtin","prid","inn","id_sp_","type_operation","price","cnt"
# #     for l in csvfile.readlines():
# #         goods.append(
# #             SoldGoods(
# #                 dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
# #                 gtin=str(l.split(",")[1]).replace('"', ""),
# #                 prid=str(l.split(",")[2]).replace('"', ""),
# #                 inn=str(l.split(",")[3]).replace('"', ""),
# #                 id_sp_=str(l.split(",")[4]).replace('"', ""),
# #                 type_operation=str(l.split(",")[5]).replace('"', ""),
# #                 price=int(l.split(",")[6]),
# #                 cnt=int(l.split(",")[7]),
# #                 user=file.user,
# #             )
# #         )
# # elif fields == '"dt","gtin","prid","sender_inn","receiver_inn","cnt_moved"\n':
# #     for l in csvfile.readlines():
# #         goods.append(
# #             TransportedGoods(
# #                 dt=datetime.strptime(l.split(",")[0], "%Y-%m-%d"),
# #                 gtin=str(l.split(",")[1]).replace('"', ""),
# #                 prid=str(l.split(",")[2]).replace('"', ""),
# #                 sender_inn=str(l.split(",")[3]).replace('"', ""),
# #                 receiver_inn=str(l.split(",")[4]).replace('"', ""),
# #                 cnt_moved=int(l.split(",")[5]),
# #                 user=file.user,
# #             )
# #         )
