import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина на платформе Ozon.
    
    Args:
        last_id (str): Идентификатор последнего элемента, используемый для получения следующей страницы.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для авторизации на API Ozon.
        
    Returns:
        dict: Словарь с информацией о товарах.
        
    Пример корректного использования:
        >>> get_product_list("0", "client_id_example", "seller_token_example")
        {"items": [...], "total": 123, ...}
    
    Пример некорректного использования:
        >>> get_product_list(123, None, "token")
        TypeError: Expected str for last_id and client_id, got int and None.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы всех товаров магазина Ozon.
    
    Args:
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для авторизации на API Ozon.
        
    Returns:
        list: Список артикулов товаров.
        
    Пример корректного использования:
        >>> get_offer_ids("client_id_example", "seller_token_example")
        ["offer_id_1", "offer_id_2", ...]
    
    Пример некорректного использования:
        >>> get_offer_ids(123, None)
        TypeError: Expected str for client_id and seller_token, got int and None.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на платформе Ozon.
    
    Args:
        prices (list): Список цен для обновления.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для авторизации на API Ozon.
        
    Returns:
        dict: Результат обновления цен.
        
    Пример корректного использования:
        >>> update_price([{"offer_id": "123", "price": "5990"}], "client_id", "token")
        {"status": "success"}
    
    Пример некорректного использования:
        >>> update_price("not_a_list", 123, None)
        TypeError: Expected list for prices and str for client_id and seller_token.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров на платформе Ozon.
    
    Args:
        stocks (list): Список остатков для обновления.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для авторизации на API Ozon.
        
    Returns:
        dict: Результат обновления остатков.
        
    Пример корректного использования:
        >>> update_stocks([{"offer_id": "123", "stock": 10}], "client_id", "token")
        {"status": "success"}
    
    Пример некорректного использования:
        >>> update_stocks("not_a_list", 123, None)
        TypeError: Expected list for stocks and str for client_id and seller_token.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать и распаковать файл остатков с сайта Casio.
    
    Returns:
        list: Список остатков с данными из файла `ostatki.xls`.
        
    Пример корректного использования:
        >>> download_stock()
        [{"Код": "123", "Количество": "10"}, ...]
    
    Пример некорректного использования:
        - Ошибка сети или отсутствия доступа к файлу вызовет requests.exceptions.RequestException.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков для обновления на Ozon.
    
    Args:
        watch_remnants (list): Список остатков товаров, загруженных с сайта Casio.
        offer_ids (list): Список артикулов товаров на Ozon.
        
    Returns:
        list: Список остатков для обновления на Ozon.
        
    Пример корректного использования:
        >>> create_stocks([{"Код": "123", "Количество": "10"}], ["123"])
        [{"offer_id": "123", "stock": 10}]
    
    Пример некорректного использования:
        >>> create_stocks("not_a_list", "not_a_list")
        TypeError: Expected list for watch_remnants and offer_ids.
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен для обновления на Ozon.
    
    Args:
        watch_remnants (list): Список остатков товаров, загруженных с сайта Casio.
        offer_ids (list): Список артикулов товаров на Ozon.
        
    Returns:
        list: Список цен для обновления на Ozon.
        
    Пример корректного использования:
        >>> create_prices([{"Код": "123", "Цена": "5990 руб."}], ["123"])
        [{"offer_id": "123", "price": "5990", "currency_code": "RUB"}]
    
    Пример некорректного использования:
        >>> create_prices("not_a_list", "not_a_list")
        TypeError: Expected list for watch_remnants and offer_ids.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену в строку с только цифрами. Пример: 5'990.00 руб. -> '5990'
    
    Args:
        price (str): Цена в строковом формате, возможно с разделителями и символами валюты.
        
    Returns:
        str: Строка, содержащая только цифры из целой части цены.
        
    Примеры корректного использования:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("1 234 руб.")
        '1234'
    
    Примеры некорректного использования:
        >>> price_conversion("текст без цифр")
        ''
        >>> price_conversion("")
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])



def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов
        
    Args:
        lst (list): Список чего-либо
        n (int): Максимальное кол-во элементов в списке
        
    Yield:
        Возвращает разделенные списки по n элментов внутри
        
    Примеры корректного использования:
        >>> lst = [1, 2, 3, 4, 5, 6, 7, 8]
        >>> n = 3
        >>> print(divide(lst: list, n: int))
        [1, 2, 3]
        [4, 5, 6]
        [7, 8]
    
    Примеры некорректного использования:
        >>> print(list(divide([], 3)))
        []
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронная загрузка цен на платформу Ozon.
    
    Args:
        watch_remnants (list): Список остатков товаров, загруженных с сайта Casio.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для авторизации на API Ozon.
        
    Returns:
        list: Список обновленных цен.
        
    Пример корректного использования:
        >>> await upload_prices([{"Код": "123", "Цена": "5990 руб."}], "client_id", "token")
        [{"offer_id": "123", "price": "5990"}]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронная загрузка остатков на платформу Ozon.
    
    Args:
        watch_remnants (list): Список остатков товаров, загруженных с сайта Casio.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для авторизации на API Ozon.
        
    Returns:
        tuple: Списки обновленных и всех остатков.
        
    Пример корректного использования:
        >>> await upload_stocks([{"Код": "123", "Количество": "10"}], "client_id", "token")
        ([{"offer_id": "123", "stock": 10}], [{"offer_id": "123", "stock": 10}])
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция, запускающая процесс обновления цен и остатков на платформе Ozon."""
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
