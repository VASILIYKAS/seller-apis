import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    
    Получить список товаров с яндекс маркета. Возвращает данные о товарах в формате JSON.

    Args:
        page (str): Номером страницы
        campaign_id (str): Уникальный номер магазина
        access_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список словарей, содержащий информацию о товарах.

    Raises:
        requests.exceptions.HTTPError: Если запрос завершился с ошибкой (например, 
                                        код ответа 4xx или 5xx).

    Examples:
        >>> page = "" 
        >>> campaign_id = "ваш_campaign_id"
        >>> access_token = "ваш_market_token"
        >>> get_product_list(page, campaign_id, access_token)
        [
            {
                "offer_id": "12345",
                "title": "Товар 1",
                "price": 1000,
                ....
            },
            {
                "offer_id": "67890",
                "title": "Товар 2",
                "price": 2000,
                ....
            },
            ....
        ]

        >>> page = "" 
        >>> campaign_id = "ваш_campaign_id"
        >>> access_token = "ваш_market_token"
        >>> get_product_list(page, campaign_id, access_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: 
        https://api.partner.market.yandex.ru/campaigns/"ваш_campaign_id"/offer-mapping-entries

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
    """
    
    Обновляет информацию о количестве товаров на складе

    Args:
        stocks (list): Список словарей содержащий информация о товарах на складах
        campaign_id (str): Уникальный номер магазина
        access_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список словарей, содержащий информацию о товарах.

    Raises:
        requests.exceptions.HTTPError: Если запрос завершился с ошибкой (например, 
                                        код ответа 4xx или 5xx).

    Examples:
        >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> campaign_id = "ваш_campaign_id"
        >>> access_token = "ваш_market_token"
        >>> update_stocks(stocks, campaign_id, access_token)
        Остатки товаров на складах.
        

        >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id) 
        >>> campaign_id = "ваш_campaign_id"
        >>> access_token = "ваш_market_token"
        >>> update_stocks(stocks, campaign_id, access_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: 
        https://api.partner.market.yandex.ru/campaigns/"ваш_campaign_id"/offers/stocks

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
    """
    
    Обновляет цену товаров

    Args:
        prices (list): Список словарей содержащий уникальный номер товара и его цену
        campaign_id (str): Уникальный номер магазина
        access_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список словарей, содержащий информацию о цене товаров.

    Raises:
        requests.exceptions.HTTPError: Если запрос завершился с ошибкой (например, 
                                        код ответа 4xx или 5xx).

    Examples:
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> campaign_id = "ваш_campaign_id"
        >>> access_token = "ваш_market_token"
        >>> update_price(prices, campaign_id, access_token)
        Список товаров.
        Товар с информацией о новой цене на него.
        

        >>> prices = create_prices(watch_remnants, offer_ids) 
        >>> campaign_id = "ваш_campaign_id"
        >>> access_token = "ваш_market_token"
        >>> update_price(prices, campaign_id, access_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: 
        https://api.partner.market.yandex.ru/campaigns/"ваш_campaign_id"/offer-prices/updates

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
    """
    
    Получить артикулы товаров с яндекс маркета

    Args:
        campaign_id (str): Уникальный номер магазина
        market_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список, содержащий уникальный номер товара (SKU)

    Raises:
        requests.exceptions.HTTPError: Если запрос завершился с ошибкой (например, 
                                        код ответа 4xx или 5xx).

    Examples:
        >>> campaign_id = "ваш_campaign_id"
        >>> market_token = "ваш_market_token"
        >>> get_offer_ids(campaign_id, market_token)
        ["123", "456", "789", "101", "121"]
        
        >>> campaign_id = "ваш_campaign_id"
        >>> market_token = "ваш_market_token"
        >>> get_offer_ids(campaign_id, market_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: 
        https://api.partner.market.yandex.ru/campaigns/"ваш_campaign_id"/offer-mapping-entries

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
    """
    
    Обновляет количестов товаров и добавляет отсутствующие товары

    Args:
        watch_remnants (str): список словарей, содержащий данные о товарах
        offer_ids (str): Список артикулов товаров с яндекс маркета
        warehouse_id (str): Уникальный номер склада, где хранится ваш товар

    Returns:
        list: Возвращает список словарей, содержащий артикли товаров и их остатки

    Examples:
        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        >>> warehouse_id = "ваш_warehouse_id"
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        [   
            {
                    "sku": 123456,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 6,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:52Z",
                        }
                    ],
            },
            {
                    "sku": 789012,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 100,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:53Z",
                        }
                    ],
            },
            ....
        ]    

        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        >>> warehouse_id = "ваш_warehouse_id"
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        ValueError: Если входные данные некорректны

    """
    # Уберем то, что не загружено в market
    stocks = list()
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
    # Добавим недостающее из загруженного:
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
    """
    
    Добавляет цену на товары

    Args:
        watch_remnants (str): список словарей, содержащий данные о товарах
        offer_ids (str): Список артикулов товаров с яндекс маркета

    Returns:
        list: Возвращает список словарей, содержащий информацию о цене товара

    Examples:
        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        >>> create_prices(watch_remnants, offer_ids)
        [   
            {
                "id": 123456,
                "price": {
                    "value": 6890,
                    "currencyId": "RUR",
            },
            {
                "id": 789101,
                "price": {
                    "value": 4990,
                    "currencyId": "RUR",
            },
            ....
        ]

        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        >>> create_prices([], offer_ids)
        []
        
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    
    Асинхронная функция, предназначена для загрузки цен
    
    Args:
        watch_remnants (list): Cписок словарей, содержащий данные о товарах
        campaign_id (str): Уникальный номер магазина 
        market_token (str): Уникальный ключ продавца для доступа к API
    
    Returns:
        list: Возвращает список словарей, содержащий информацию о цене товара

    Examples:
        >>> watch_remnants = download_stock()
        >>> campaign_id = "ваш_campaign_id"
        >>> market_token = "ваш_market_token"
        >>> upload_prices(watch_remnants, campaign_id, market_token)
        [   
            {
                "id": 123456,
                "price": {
                    "value": 6890,
                    "currencyId": "RUR",
            },
            {
                "id": 789101,
                "price": {
                    "value": 4990,
                    "currencyId": "RUR",
            },
            ....
        ]

        >>> watch_remnants = download_stock()
        >>> campaign_id = "ваш_campaign_id"
        >>> market_token = "ваш_market_token"
        >>> upload_prices(watch_remnants, campaign_id, market_token)
        []

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices



async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    
    Асинхронная функция, предназначена для загрузки обновленного количества товаров
    
    Args:
        watch_remnants (list): Cписок словарей, содержащий данные о товарах
        campaign_id (str): Уникальный номер магазина 
        market_token (str): Уникальный ключ продавца для доступа к API
        warehouse_id (str): Уникальный номер склада, где хранится ваш товар

    Returns:
        list: Возвращает список словарей, содержащий артикли товаров, их остатки,
        номер склада, состояние товара и дату обновления

    Examples:
        >>> watch_remnants = download_stock()
        >>> campaign_id = "ваш_campaign_id"
        >>> market_token = "ваш_market_token"
        >>> upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        [   
            {
                    "sku": 123456,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 6,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:52Z",
                        }
                    ],
            },
            {
                    "sku": 789012,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 100,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:53Z",
                        }
                    ],
            },
            ....
        ],
        [   
            {
                    "sku": 123456,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 6,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:52Z",
                        }
                    ],
            },
            {
                    "sku": 789012,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 100,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:53Z",
                        }
                    ],
            },
            {
                    "sku": 131415,
                    "warehouseId": 1122334455,
                    "items": [
                        {
                            "count": 0,
                            "type": "FIT",
                            "updatedAt": "2025-01-20T17:14:53Z",
                        }
                    ],
            },
            ....
        ]  
        
        >>> watch_remnants = download_stock()
        >>> campaign_id = "ваш_campaign_id"
        >>> market_token = "ваш_market_token"
        >>> upload_stocks([], campaign_id, market_token, warehouse_id)
        [], []

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
