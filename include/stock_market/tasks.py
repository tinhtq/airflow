import json
from io import BytesIO
from minio import Minio
import requests
from airflow.hooks.base import BaseHook


def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )
    return client


def _get_stock_prices(url, symbol):
    api = BaseHook.get_connection("stock_api")
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    print(url)
    response = requests.get(url, headers=api.extra_dejson["headers"])
    return json.dumps(response.json()["chart"]["result"][0])


def _store_prices(prices):
    client = _get_minio_client()

    prices = json.loads(prices)
    bucket_name = "stock-market"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    symbol = prices["meta"]["symbol"]
    data = json.dumps(prices, ensure_ascii=False).encode("utf8")
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )
    return f"{objw.bucket_name}/{symbol}"


def _get_formatted_csv(path):
    return 0
