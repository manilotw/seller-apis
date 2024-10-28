import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров для заданной кампании.

    Args:
        page (str): Токен страницы для пагинации.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа к API.

    Returns:
        list: Список товаров в кампании.

    Примеры:
        Корректное использование:
        >>> product_list = get_product_list("", "12345", "ваш_токен")

        Некорректное использование:
        >>> product_list = get_product_list("", None, "ваш_токен")
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров для заданной кампании.

    Args:
        stocks (list): Список остатков товаров.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа к API.

    Returns:
        dict: Ответ API после обновления остатков.

    Примеры:
        Корректное использование:
        >>> update_response = update_stocks(stocks, "12345", "ваш_токен")

        Некорректное использование:
        >>> update_response = update_stocks(None, "12345", "ваш_токен")
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров для заданной кампании.

    Args:
        prices (list): Список цен товаров.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа к API.

    Returns:
        dict: Ответ API после обновления цен.

    Примеры:
        Корректное использование:
        >>> update_response = update_price(prices, "12345", "ваш_токен")

        Некорректное использование:
        >>> update_response = update_price(None, "12345", "ваш_токен")
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета.

    Args:
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен доступа к API.

    Returns:
        list: Список артикулов товаров.

    Примеры:
        Корректное использование:
        >>> offer_ids = get_offer_ids("12345", "ваш_токен")

        Некорректное использование:
        >>> offer_ids = get_offer_ids(None, "ваш_токен")
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать остатки для обновления.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.
        warehouse_id (str): Идентификатор склада.

    Returns:
        list: Список остатков для обновления.

    Примеры:
        Корректное использование:
        >>> stocks = create_stocks(watch_remnants, offer_ids, "ваш_склад_id")

        Некорректное использование:
        >>> stocks = create_stocks([], None, "ваш_склад_id")
    """
    stocks = []
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цены для обновления.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список цен для обновления.

    Примеры:
        Корректное использование:
        >>> prices = create_prices(watch_remnants, offer_ids)

        Некорректное использование:
        >>> prices = create_prices([], None)
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    "currencyId": "RUR",
                },
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить цены для кампании.

    Args:
        watch_remnants (list): Список остатков товаров.
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен доступа к API.

    Returns:
        list: Список обновленных цен.

    Примеры:
        Корректное использование:
        >>> prices = await upload_prices(watch_remnants, "12345", "ваш_токен")

        Некорректное использование:
        >>> prices = await upload_prices([], None, "ваш_токен")
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить остатки для кампании.

    Args:
        watch_remnants (list): Список остатков товаров.
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен доступа к API.
        warehouse_id (str): Идентификатор склада.

    Returns:
        tuple: Кортеж из списка непустых остатков и полного списка остатков.

    Примеры:
        Корректное использование:
        >>> not_empty, all_stocks = await upload_stocks(watch_remnants, "12345", "ваш_токен", "ваш_склад_id")

        Некорректное использование:
        >>> not_empty, all_stocks =
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
