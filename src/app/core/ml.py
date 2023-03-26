import pandas as pd
import json

from etna.datasets.tsdataset import TSDataset

from etna.pipeline.pipeline import Pipeline

from etna.transforms import TimeSeriesImputerTransform

from app.core.settings import settings
from app.utils.logging import log

# region Yarik

PIPELINE_PATH = r"src/models"
REGION_CODES = {i["geoname_code"]: i for i in json.load(open("src/regions.json", "r"))}


class Model:
    TARGET_COLUMNS = {
        "sold_volume": "sum_price",
        "sold_count": "cnt",
    }
    MANUFACTURER_COLUMNS = {"sold_volume": "price", "sold_count": "cnt"}

    def _predict(self, pipe):
        forecast = pipe.forecast()
        outp_df = (
            forecast.to_pandas()
            .stack()
            .reset_index()
            .drop("feature", axis=1)
            .melt("timestamp")
        )
        return outp_df

    def agg_predict(self, data: dict, pipeline_path: str, target: str) -> dict:
        """
        data: dict - data for predict
        pipeline_path: str - path to model pipeline
        target: str - sold_volume or sold_count
        """
        ts = self._procces_input(data, self.TARGET_COLUMNS[target])
        pipe = Pipeline.load(pipeline_path, ts=ts)

        outp_df = self._predict(pipe)
        outp_df = outp_df.rename(
            columns={"timestamp": "dt", "target": target, "segment": "region_code"}
        )

        return outp_df.to_dict(orient="records")

    def manufacturer_predict(
        self, data, sale_points, pipeline_path: str, target: str
    ) -> dict:
        """
        data: dict - data for predict
        sale_points: dict - data about sale points
        pipeline_path: str - path to model pipeline
        target: str - sold_volume or sold_count
        """
        log.info("Preparing data")
        data = self._region_agg(data, sale_points, self.TARGET_COLUMNS[target])
        ts = self._procces_input(data, self.TARGET_COLUMNS[target], dropna=True)
        ts.fit_transform(
            [
                TimeSeriesImputerTransform(
                    in_column="target", strategy="running_mean", window=3
                )
            ]
        )
        pipe = Pipeline.load(pipeline_path, ts=ts)

        log.info("Fitting model")
        pipe = pipe.fit(ts)
        outp_df = self._predict(pipe)
        outp_df = outp_df.rename(
            columns={"timestamp": "dt", "target": target, "segment": "region_code"}
        )

        log.info("Returning prediction")
        return outp_df.to_dict(orient="records")

    def volume_agg_predict(self, data: dict) -> dict:
        """
        data: dict - data for predict

        """
        return self.agg_predict(
            data=data,
            pipeline_path=PIPELINE_PATH + "/pipe_volume.zip",
            target="sold_volume",
        )

    def count_agg_predict(self, data: dict) -> dict:
        """
        data: dict - data for predict

        """
        return self.agg_predict(
            data=data,
            pipeline_path=PIPELINE_PATH + "/pipe_count.zip",
            target="sold_count",
        )

    def volume_manufacturer_predict(
        self,
        data: dict,
        sale_points: dict,
    ) -> dict:
        """
        data: dict - data for predict
        sale_points: dict - data about sale points

        """
        return self.manufacturer_predict(
            data=data,
            sale_points=sale_points,
            pipeline_path=PIPELINE_PATH + "/pipe_manufacturer_volume.zip",
            target="sold_volume",
        )

    def count_manufacturer_predict(
        self,
        data: dict,
        sale_points: dict,
    ) -> dict:
        """
        data: dict - data for predict
        sale_points: dict - data about sale points
        """
        return self.manufacturer_predict(
            data=data,
            sale_points=sale_points,
            pipeline_path=PIPELINE_PATH + "/pipe_manufacturer_count.zip",
            target="sold_count",
        )

    def _procces_input(self, data, target_column, dropna=False):
        df = pd.DataFrame(data)
        df = df.rename(
            columns={
                "dt": "timestamp",
                target_column: "target",
                "region_code": "segment",
            }
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        log.info(df.columns)
        df = df[["timestamp", "target", "segment"]]

        df = df.dropna()
        df = df.drop_duplicates().reset_index(drop=True)

        df = (
            df.groupby(["segment", "timestamp"])
            .aggregate("sum")
            .reset_index(level=[0, 1])
        )
        # tmp = df.groupby('segment')['timestamp'].count().sort_values()
        # df = df[df['segment'].apply(lambda x: tmp[x] > 180)]

        ts = TSDataset.to_dataset(df)
        if dropna:
            ts = ts.dropna(axis=1)
        else:
            ts = ts.fillna(0)

        ts = TSDataset(ts, freq="D")

        return ts

    def _region_agg(self, data, sale_points, target_column: str):
        data = pd.DataFrame(data)
        sale_points = pd.DataFrame(sale_points)
        sale_points["region_code"] = sale_points["region_code"].astype(object)
        data["sum_price"] = data["price"] * data["cnt"]

        data = data.merge(sale_points, "left", "id_sp_")

        data = data.dropna()
        data = data.groupby(["region_code", "dt"])[target_column].sum().reset_index()
        data["dt"] = pd.to_datetime(data["dt"])
        data = data.sort_values("dt")
        return data


# endregion Yarik

# region Ivan
def shops_manufacturer(dict1: dict, dict2: dict) -> dict:
    """Торговые точки по регионам, которые чаще всего выводят товары из оборота
    для 1 производителя"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "type_operation"]]
    dop_data.dropna(inplace=True)
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    tab = dop_data_merged[
        dop_data_merged["type_operation"] == "Прочий тип вывода из оборота"
    ]
    groups = tab.groupby(["region_code", "id_sp_"]).count().reset_index()
    shop_id = groups.sort_values(by=["dt"], ascending=False)[
        ["region_code", "id_sp_", "dt"]
    ]

    info = dict()
    for i in map(int, shop_id["region_code"].unique()):
        data = shop_id[shop_id["region_code"] == i][:5][["id_sp_", "dt"]]
        id = list(map(str, data["id_sp_"].values))
        dt = list(map(int, data["dt"].values))
        info[i] = {
            "id": id,  # id магазина (object)
            "count": dt,
        }  # кол-во выведенных из оборота товаров (int)

    return info


def volumes_manufacturer_region(dict1, dict2):
    """от 0 до 1 для heatmap"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "cnt", "price"]]
    dop_data.dropna(inplace=True)
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-09-01"]
    dop_data_merged["sum_price"] = dop_data_merged["price"] * dop_data_merged["cnt"]
    dop_data_new = dop_data_merged.drop(columns=["id_sp_", "price"])
    dop_data_region = dop_data_new.groupby(["dt", "region_code"]).sum().reset_index()
    dop_data_region["Месяц"] = dop_data_region["dt"].dt.month
    data_month_region = (
        dop_data_region.groupby(["region_code", "Месяц"])
        .sum()[["cnt", "sum_price"]]
        .reset_index()
    )
    data_month = (
        dop_data_region.groupby(["Месяц"]).sum()[["cnt", "sum_price"]].reset_index()
    )
    sum_price1 = sum(list(map(int, data_month["sum_price"].values)))
    cnt1 = sum(list(map(int, data_month["cnt"].values)))

    regions = REGION_CODES.copy()
    for i in map(int, data_month_region["region_code"].unique()):
        data = data_month_region[data_month_region["region_code"] == i]
        sum_price = list(map(int, data["sum_price"].values))
        cnt = list(map(int, data["cnt"].values))
        regions[i]["cnt"] = sum(cnt)
        regions[i]["sum"] = sum(sum_price)
        regions[i]["norm_sum"] = sum(sum_price) / sum_price1
        regions[i]["cnt_norm"] = sum(cnt) / cnt1

    df = pd.DataFrame(regions)
    df.loc["norm_sum"] = (df.loc["norm_sum"] - min(df.loc["norm_sum"])) / (
        max(df.loc["norm_sum"]) - min(df.loc["norm_sum"])
    )
    df.loc["cnt_norm"] = (df.loc["cnt_norm"] - min(df.loc["cnt_norm"])) / (
        max(df.loc["cnt_norm"]) - min(df.loc["cnt_norm"])
    )
    df.loc["norm_sum"] = df.loc["norm_sum"].apply(lambda x: round(x, 3))
    df.loc["cnt_norm"] = df.loc["cnt_norm"].apply(lambda x: round(x, 3))
    log.info(df)
    df.fillna("")
    log.info(df)
    return df.to_dict()


def volumes_manufacturer(dict1: dict, dict2: dict) -> dict:  # не нужно
    """Количество единиц товара и стоимость всего проданного товара для 1 производителя в целом
    +
    """

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "cnt", "price"]]
    dop_data.dropna(inplace=True)
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["sum_price"] = dop_data_merged["price"] * dop_data_merged["cnt"]
    dop_data_new = dop_data_merged.drop(columns=["id_sp_", "price"])
    dop_data_region = dop_data_new.groupby(["dt", "region_code"]).sum().reset_index()
    dop_data_region["Месяц"] = dop_data_region["dt"].dt.month
    data_month = (
        dop_data_region.groupby(["Месяц"]).sum()[["cnt", "sum_price"]].reset_index()
    )

    data = data_month.copy()
    month = list(map(int, data["Месяц"].values))
    count = list(map(int, data["cnt"].values))
    sum_price = list(map(int, data["sum_price"].values))
    info = {
        "month": month,  # месяц
        "count": count,  # кол-во выведенного из оборота товара
        "sum_price": sum_price,
    }  # суммарная цена товаров

    return info


def popular_offline_gtin_manufacturer_region(dict1: dict, dict2: dict) -> dict:
    """Самые популярные товары среди оффлайн покупателей
    для 1 производителя по регионам
    +
    """

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "gtin", "cnt", "price", "type_operation"]]
    dop_data.dropna(inplace=True)
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["sum_price"] = dop_data_merged["price"] * dop_data_merged["cnt"]
    tab = dop_data_merged[
        dop_data_merged["type_operation"]
        == "Продажа конечному потребителю в точке продаж"
    ]
    tab.drop(columns=["price"], inplace=True)
    groups = tab.groupby(["region_code", "gtin"]).sum().reset_index()
    popular = groups.sort_values(by=["cnt"], ascending=False)[
        ["region_code", "gtin", "cnt"]
    ]

    info_popular = dict()
    for i in map(int, popular["region_code"].unique()):
        data = popular[popular["region_code"] == i][:5][["gtin", "cnt"]]
        gtin = list(map(str, data["gtin"].values))
        cnt = list(map(int, data["cnt"].values))
        info_popular[i] = {
            "gtin": gtin,  # gtin товара
            "count": cnt,
        }  # кол-во товара проданного оффлайн

    return info_popular


def popular_offline_gtin_manufacturer(dict1: dict, dict2: dict) -> dict:
    """Самые популярные товары среди оффлайн покупателей
    для 1 производителя в целом
        +
    """

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "gtin", "cnt", "price", "type_operation"]]
    dop_data.dropna(inplace=True)
    shops = shops[["id_sp_", "region_code"]]
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["sum_price"] = dop_data_merged["price"] * dop_data_merged["cnt"]
    tab = dop_data_merged[
        dop_data_merged["type_operation"]
        == "Продажа конечному потребителю в точке продаж"
    ]
    tab.drop(columns=["price"], inplace=True)
    groups = tab.groupby(["gtin"]).sum().reset_index()
    popular = groups.sort_values(by=["cnt"], ascending=False)[["gtin", "cnt"]]

    data = popular[:5][["gtin", "cnt"]]
    gtin = list(map(str, data["gtin"].values))
    cnt = list(map(int, data["cnt"].values))
    info_popular = {
        "gtin": gtin,  # gtin товара
        "count": cnt,
    }  # кол-во товара проданного оффлайн

    return info_popular


def popular_online_gtin_manufacturer(dict1: dict) -> dict:
    """Самые популярные товары среди онлайн покупателей
    для 1 производителя
    +
    """

    dop_data = pd.DataFrame(dict1)
    dop_data_merged = dop_data[["dt", "cnt", "type_operation", "gtin"]]
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    tab = dop_data_merged[
        dop_data_merged["type_operation"]
        == "Дистанционная продажа конечному потребителю"
    ]
    groups = tab.groupby(["gtin"]).sum().reset_index()
    popular = groups.sort_values(by=["cnt"], ascending=False)[["gtin", "cnt"]]

    data = popular.iloc[:10][["gtin", "cnt"]]
    gtin = list(map(str, data["gtin"].values))
    cnt = list(map(int, data["cnt"].values))
    info_popular = {
        "gtin": gtin,  # gtin товара
        "count": cnt,
    }  # кол-во товара проданного онлайн

    return info_popular


def shops_manufacturer_count_region(dict1: dict, dict2: dict) -> dict:
    """Количество торговых точек по регионам для 1 производителя"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "cnt"]]
    dop_data.dropna(inplace=True)
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["Месяц"] = dop_data_merged["dt"].dt.month
    groups = (
        dop_data_merged.groupby(["region_code", "id_sp_", "Месяц"])
        .count()
        .reset_index()
    )
    count_shops = groups.groupby(["region_code", "Месяц"]).count()["cnt"].reset_index()

    info_popular = dict()
    for i in map(int, count_shops["region_code"].unique()):
        data = count_shops[count_shops["region_code"] == i][["Месяц", "cnt"]]
        month = list(map(int, data["Месяц"].values))
        cnt = list(map(int, data["cnt"].values))
        info_popular[i] = {
            "region_code": i,
            "month": month,
            "count": cnt,
        }  # месяц  # кол-во магазинов

    return info_popular


def shops_manufacturer_count(dict1: dict, dict2: dict) -> dict:
    """Количество торговых точек в целом для 1 производителя"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    shops = shops[["id_sp_", "region_code"]]
    dop_data = dop_data[["dt", "id_sp_", "cnt"]]
    dop_data.dropna(inplace=True)
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["Месяц"] = dop_data_merged["dt"].dt.month
    groups = dop_data_merged.groupby(["id_sp_", "Месяц"]).count().reset_index()
    count_shops = groups.groupby(["Месяц"]).count()["cnt"].reset_index()

    data = count_shops.copy()
    month = list(map(int, data["Месяц"].values))
    count = list(map(int, data["cnt"].values))
    info = {"month": month, "count": count}  # месяц  # кол-во магазинов

    return info


# endregion Ivan
