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
    """
    
    Получить список товаров магазина озон.

    Args:
        last_id (str): Идентификатор последнего товара
        client_id (str): Идентификатор клиента для проверки подлинности пользователя
        seller_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        dict: Возвращает словарь, содержащий информацию о товарах.

    Raises:
        requests.exceptions.HTTPError: Если запрос завершился с ошибкой (например, 
                                        код ответа 4xx или 5xx).

    Examples:
        >>> last_id = "" 
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> get_product_list(last_id, client_id, seller_token)
        {
            "result": {
                "items": [
                    {
                        "product_id": 223681945,
                        "offer_id": "136748"
                    }
                ],
                "total": 1,
                "last_id": "bnVсbA=="
            }
        }

        >>> last_id = "" 
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> get_product_list(last_id, client_id, seller_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: https://api-seller.ozon.ru/v2/product/list

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
    """
    
    Получить артикулы товаров магазина озон.

    Args:
        client_id (str): Идентификатор клиента для проверки подлинности пользователя
        seller_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список, содержащий артикулы товаров.

    Examples:
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> get_offer_ids(client_id, seller_token)
        ["136748", "137239", "137397" ....]
   
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> get_offer_ids(client_id, seller_token)
        []

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
    """
    
    Обновляет цены товаров, используя запрос к API.

    Args:
        prices (list): Cписок цен для обновления
        client_id (str): Идентификатор клиента для проверки подлинности пользователя
        seller_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        dict: Возвращает словарь, содержащий результаты обновления цен для каждого товара.

    Examples:
        >>> prices = [2590, 14200, 29990]
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> update_price(prices, client_id, seller_token)
        {
            "result": [
                {
                    "product_id": 1386,
                    "offer_id": "PH8865",
                    "updated": true,
                    "errors": [ ]
                }
            ]
        }
   
        >>> prices = [2590, 14200, 29990]
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> update_price(prices, client_id, seller_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: https://api-seller.ozon.ru/v1/product/import/prices

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
    """
    
    Обновляет информацию о количестве товаров на складе

    Args:
        stocks (list): Список словарей содержащий информацию о товарах на складах
        client_id (str): Идентификатор клиента для проверки подлинности пользователя
        seller_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        dict: Возвращает словарь, содержащий результаты обновления остатков для каждого товара.

    Examples:
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> update_stocks(stocks, client_id, seller_token)
        {
            "result": [
                {
                    "warehouse_id": 22142605386000,
                    "product_id": 118597312,
                    "quant_size": 1,
                    "offer_id": "PH11042",
                    "updated": true,
                    "errors": []
                }
            ]
        }

        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> update_stocks(stocks, client_id, seller_token)
        requests.exceptions.HTTPError: 400 Client Error: 
        Bad Request for url: https://api-seller.ozon.ru/v1/product/import/prices

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
    """
    
    Скачивает файл содержащий информацию об остатках товаров с сайта casio 
    и сохраняет его содержимое в переменную

    Returns:
        list: Возвращает список словарей, содержащий данные о товарах

    Examples:
        
        >>> download_stock()
        [
        {'Заказ': '',
        'Изображение': 'Показать',
        'Код': 63433,
        'Количество': '>10',
        'Наименование товара': 'Q422-010Y RUS',
        'Цена': "2'240.00 руб."},
        {'Заказ': '',
        'Изображение': 'Показать',
        'Код': 67210,
        'Количество': '>10',
        'Наименование товара': 'Q422-201Y RUS',
        'Цена': "1'810.00 руб."},
        ....
        ]

        >>> download_stock()
        FileNotFoundError: [Errno 2] No such file or directory
        
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
    """
    
    Обновляет количестов товаров и добавляет отсутствующие товары

    Args:
        watch_remnants (list): Cписок словарей, содержащий данные о товарах 
        offer_ids (list): Список артикулов товаров


    Returns:
        list: Возвращает список словарей, содержащий артикли товаров и их остатки.

    Examples:
        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> create_stocks(watch_remnants, offer_ids)
        [
            {"offer_id": 136748, "stock": 6},
            {"offer_id": 136749, "stock": 100},
            {"offer_id": 136750, "stock": 0},
            ....
        ]

        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> create_stocks([], offer_ids)
        [
            {"offer_id": 136748, "stock": 0},
            {"offer_id": 136749, "stock": 0},
            {"offer_id": 136750, "stock": 0},
            ....
        ]

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
    """
    
    Добавляет цену на товары

    Args:
        watch_remnants (list): Cписок словарей, содержащий данные о товарах 
        offer_ids (list): Список артикулов товаров

    Returns:
        list: Возвращает список словарей, содержащий информацию о цене товара

    Examples:
        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "136748",
                "old_price": "0",
                "price": "9990",
            },
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "136749",
                "old_price": "0",
                "price": "7250",
            },
            ....
        ]

        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> create_prices([], ["136749"])
        []

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
    """
    
    Изменяет формат цены.

    Args:
        price (str): Цена товара в строковом формате.

    Returns:
        str: Возвращает отформатированную цену, содержащию только целые числа.

    Examples:
        >>> price="5'990.00 руб" 
        >>> price_conversion(price)
        '5990'

        >>> price = "abc"
        >>> price_conversion(price)
        ''

        >>> price = 1000
        >>> price_conversion(price)
        TypeError: expected string or bytes
    
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    
    Разделить список lst на части по n элементов
    
    Args:
        lst (list): Список который нужно разбить
        n (int): Количество элементов в каждой части

    Examples:
        >>> lst = [1, 2, 3, 4, 5, 6]
        >>> n = 2
        >>> divide(lst, 2)
        [
            [1, 2],
            [3, 4],
            [5, 6],
        ]

        >>> lst = [1, 2, 3, 4, 5, 6]
        >>> n = 0
        >>> divide(lst, 2)
        ValueError: range() arg 3 must not be zero

        >>> lst = [1, 2, 3, 4, 5, 6]
        >>> n = -2
        >>> divide(lst, 2)
        []

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    
    Асинхронная функция, предназначена для загрузки цен
    
    Args:
        watch_remnants (list): Cписок словарей, содержащий данные о товарах
        client_id (str): Идентификатор клиента для проверки подлинности пользователя
        seller_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список словарей, содержащий информацию о цене товара

    Examples:
        >>> watch_remnants = download_stock()
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> upload_prices(watch_remnants, client_id, seller_token)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "136748",
                "old_price": "0",
                "price": "9990",
            },
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "136749",
                "old_price": "0",
                "price": "7250",
            },
            ....
        ]

        >>> watch_remnants = download_stock()
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> upload_prices([], client_id, seller_token)
        []

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    
    Асинхронная функция, предназначена для загрузки обновленного количества товаров
    
    Args:
        watch_remnants (list): Cписок словарей, содержащий данные о товарах
        client_id (str): Идентификатор клиента для проверки подлинности пользователя
        seller_token (str): Уникальный ключ продавца для доступа к API

    Returns:
        list: Возвращает список словарей, содержащий артикли товаров и их остатки

    Examples:
        >>> watch_remnants = download_stock()
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> upload_stocks(watch_remnants, client_id, seller_token)
        [
            {"offer_id": 136748, "stock": 6},
            {"offer_id": 136749, "stock": 100},
            {"offer_id": 136750, "stock": 0},
            ....
        ],
        [
            {"offer_id": 136748, "stock": 6},
            {"offer_id": 136749, "stock": 100},
            {"offer_id": 136750, "stock": 0},
            ....
        ]

        >>> watch_remnants = download_stock()
        >>> client_id = "ваш_client_id"
        >>> seller_token = "ваш_seller_token"
        >>> upload_stocks([], client_id, seller_token)
        [], []

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
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
