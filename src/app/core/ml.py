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
        id = data["id_sp_"].values
        dt = data["dt"].values
        info[i] = {
            "id": id,  # id магазина
            "count": dt,
        }  # кол-во выведенных из оборота товаров

    return info
