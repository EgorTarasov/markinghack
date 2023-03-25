import pandas as pd

from etna.datasets.tsdataset import TSDataset

from etna.pipeline.pipeline import Pipeline

from app.core.settings import settings

# region Yarik


class Model:
    def predict(self, data) -> dict:
        self.ts = self._procces_input(data)
        pipe = Pipeline.load(settings.PIPELINE_PATH, ts=self.ts)
        forecast = pipe.forecast()
        outp_df = (
            forecast.to_pandas()
            .stack()
            .reset_index()
            .drop("feature", axis=1)
            .melt("timestamp")
        )
        outp_df = outp_df.rename(
            columns={"timestamp": "dt", "target": "sum_price", "segment": "region_code"}
        )
        return outp_df.to_dict(orient="list")

    def _procces_input(self, data):
        df = pd.DataFrame(data)
        df = df.rename(
            columns={"dt": "timestamp", "sum_price": "target", "region_code": "segment"}
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[["timestamp", "target", "segment"]]

        df = df.dropna()
        df = df.drop_duplicates().reset_index(drop=True)

        df = (
            df.groupby(["segment", "timestamp"])
            .aggregate("sum")
            .reset_index(level=[0, 1])
        )
        # tmp = df.groupby('segment')['timestamp'].count().sort_values()
        # df = df[df['segment'].apply(lambda x: tmp[x] > 90)]

        ts = TSDataset.to_dataset(df)
        ts = TSDataset(ts, freq="D")

        return ts


# endregion Yarik


def shops_manufacturer(dict1: dict, dict2: dict) -> dict:
    """Торговые точки по регионам, которые чаще всего выводят товары из оборота
    для 1 производителя"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    dop_data.dropna(inplace=True)
    shops = shops[["id_sp_", "region_code", "city_with_type"]]
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
    for i in shop_id["region_code"].unique():
        data = shop_id[shop_id["region_code"] == i][:5][["id_sp_", "dt"]]
        id = list(map(str, data["id_sp_"].values))
        dt = list(map(int, data["dt"].values))
        info[i] = {
            "id": id,  # id магазина (object)
            "count": dt,
        }  # кол-во выведенных из оборота товаров (int)

    return info


def volumes_manufacturer_region(dict1: dict, dict2: dict) -> dict:
    """Количество единиц товара и стоимость всего проданного товара для 1 производителя по регионам"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    dop_data.dropna(inplace=True)
    shops = shops[["id_sp_", "region_code", "city_with_type"]]
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["sum_price"] = dop_data_merged["price"] * dop_data_merged["cnt"]
    dop_data_new = dop_data_merged.drop(columns=["prid", "inn", "id_sp_", "price"])
    dop_data_new.drop(columns=["city_with_type"], inplace=True)
    dop_data_region = dop_data_new.groupby(["dt", "region_code"]).sum().reset_index()
    dop_data_region["Месяц"] = dop_data_region["dt"].dt.month
    data_month_region = (
        dop_data_region.groupby(["region_code", "Месяц"])
        .sum()[["cnt", "sum_price"]]
        .reset_index()
    )

    info = dict()
    for i in data_month_region["region_code"].unique():
        data = data_month_region[data_month_region["region_code"] == i]
        month = list(data["Месяц"].astype("int").values)
        count = list(data["cnt"].astype("int").values)
        sum_price = list(data["sum_price"].astype("int").values)
        info[i] = {
            "month": month,  # месяц
            "count": count,  # кол-во выведенного из оборота товара
            "sum_price": sum_price,
        }  # суммарная цена товаров

    return info


def volumes_manufacturer(dict1: dict, dict2: dict) -> dict:
    """Количество единиц товара и стоимость всего проданного товара для 1 производителя в целом"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
    dop_data.dropna(inplace=True)
    shops = shops[["id_sp_", "region_code", "city_with_type"]]
    dop_data_merged = pd.merge(dop_data, shops, on="id_sp_", how="left")
    dop_data_merged["dt"] = pd.to_datetime(dop_data_merged["dt"])
    dop_data_merged.sort_values(by=["dt"], inplace=True)
    dop_data_merged = dop_data_merged[dop_data_merged["dt"] >= "2022-01-01"]
    dop_data_merged["sum_price"] = dop_data_merged["price"] * dop_data_merged["cnt"]
    dop_data_new = dop_data_merged.drop(columns=["prid", "inn", "id_sp_", "price"])
    dop_data_new.drop(columns=["city_with_type"], inplace=True)
    dop_data_region = dop_data_new.groupby(["dt", "region_code"]).sum().reset_index()
    dop_data_region["Месяц"] = dop_data_region["dt"].dt.month
    data_month = (
        dop_data_region.groupby(["Месяц"]).sum()[["cnt", "sum_price"]].reset_index()
    )

    data = data_month.copy()
    month = list(data["Месяц"].astype("int").values)
    count = list(data["cnt"].astype("int").values)
    sum_price = list(data["sum_price"].astype("int").values)
    info = {
        "month": month,  # месяц
        "count": count,  # кол-во выведенного из оборота товара
        "sum_price": sum_price,
    }  # суммарная цена товаров

    return info


def popular_offline_gtin_manufacturer_region(dict1: dict, dict2: dict) -> dict:
    """Самые популярные товары среди оффлайн покупателей
    для 1 производителя по регионам"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
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
    groups = tab.groupby(["region_code", "gtin"]).sum().reset_index()
    popular = groups.sort_values(by=["cnt"], ascending=False)[
        ["region_code", "gtin", "cnt"]
    ]

    info_popular = dict()
    for i in popular["region_code"].unique():
        data = popular[popular["region_code"] == i][:5][["gtin", "cnt"]]
        gtin = list(data["gtin"].astype("str").values)
        cnt = list(data["cnt"].astype("int").values)
        info_popular[i] = {
            "gtin": gtin,  # gtin товара
            "count": cnt,
        }  # кол-во товара проданного оффлайн

    return info_popular


def popular_offline_gtin_manufacturer(dict1: dict, dict2: dict) -> dict:
    """Самые популярные товары среди оффлайн покупателей
    для 1 производителя в целом"""

    dop_data = pd.DataFrame(dict1)
    shops = pd.DataFrame(dict2)
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
    gtin = list(data["gtin"].astype("str").values)
    cnt = list(data["cnt"].astype("int").values)
    info_popular = {
        "gtin": gtin,  # gtin товара
        "count": cnt,
    }  # кол-во товара проданного оффлайн

    return info_popular
