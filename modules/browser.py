from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from base64 import b64encode, b64decode

from curl_cffi.requests import AsyncSession
from requests import get, post
from datetime import datetime
from time import time, sleep
from json import dumps
import asyncio

from modules.retry import async_retry, retry, have_json
from modules.utils import logger, sleeping
from modules.database import DataBase
import settings


class Browser:

    BACKPACK_API: str = "https://api.backpack.exchange/api/v1"

    def __init__(
            self,
            api_key: str,
            encoded_api_key: str,
            label: str,
            db: DataBase,
            proxy: str,
            custom_session: bool = False,
    ):
        self.max_retries = 5
        self.db = db
        self.encoded_api_key = encoded_api_key
        self.api_key = api_key
        self.label = label
        self.private_key = Ed25519PrivateKey.from_private_bytes(b64decode(api_key.split(':')[1]))

        if proxy is None:
            self.proxy = None
        elif proxy == "mobile":
            self.proxy = settings.PROXY if settings.PROXY not in ['http://log:pass@ip:port', '', None] else None
        else:
            self.proxy = "http://" + proxy.removeprefix("https://").removeprefix("http://")

        if not custom_session:
            if self.proxy:
                logger.debug(f'[•] {self.label} | Soft | Got proxy {self.proxy}')
                if proxy == "mobile":
                    self.change_ip()
            else:
                logger.warning(f'[•] {self.label} | Soft | You dont use proxies')

        self.session = self.get_new_session()

    def get_new_session(self):
        session = AsyncSession(
            impersonate="chrome131",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.1",
                "Origin": "https://backpack.exchange",
                "Referer": "https://backpack.exchange/",
            }
        )
        if self.proxy not in ['http://log:pass@ip:port', '', None]:
            session.proxies.update({'http': self.proxy, 'https': self.proxy})

        return session


    @have_json
    async def send_request(self, **kwargs):
        if kwargs.get("api_instruction") is not None:
            headers = kwargs.get("headers", {})
            headers.update(
                self.build_headers(
                    kwargs["api_instruction"],
                    {**kwargs.get("params", {}), **kwargs.get("json", {})},
                )
            )
            kwargs["headers"] = headers
            del kwargs["api_instruction"]

        if kwargs.get("session"):
            session = kwargs["session"]
            del kwargs["session"]
        else:
            session = self.session

        if kwargs.get("method"): kwargs["method"] = kwargs["method"].upper()
        return await session.request(**kwargs)


    def change_ip(self):
        if settings.CHANGE_IP_LINK not in ['https://changeip.mobileproxy.space/?proxy_key=...&format=json', '']:
            print('')
            while True:
                try:
                    r = get(settings.CHANGE_IP_LINK)
                    if 'mobileproxy' in settings.CHANGE_IP_LINK and r.json().get('status') == 'OK':
                        logger.debug(f'[+] {self.label} | Proxy | Successfully changed ip: {r.json()["new_ip"]}')
                        return True
                    elif not 'mobileproxy' in settings.CHANGE_IP_LINK and r.status_code == 200:
                        logger.debug(f'[+] {self.label} | Proxy | Successfully changed ip: {r.text}')
                        return True
                    logger.error(f'[-] {self.label} | Proxy | Change IP error: {r.text} | {r.status_code}')
                    sleep(10)

                except Exception as err:
                    logger.error(f'[-] {self.label} | Browser | Cannot get proxy: {err}')

    def build_headers(self, method: str, params: dict):
        ts = str(int(time() * 1e3))
        window = "5000"
        api_key, api_secret = self.api_key.split(':')

        body = {
            key: dumps(value) if type(value) == bool else value
            for key, value in sorted(params.items())
        }
        body.update({
            "timestamp": ts,
            "window": window,
        })
        instruction = f"instruction={method}&" if method else ""
        str_body = instruction + "&".join(f"{key}={value}" for key, value in body.items())
        signature = self.private_key.sign(str_body.encode())
        encoded_signature = b64encode(signature).decode()

        res = {
            "X-Timestamp": ts,
            "X-Window": window,
            "X-API-Key": api_key,
            "X-Signature": encoded_signature,
        }

        return res

    @async_retry(source="Browser", module_str="Get Account Info", exceptions=Exception)
    async def get_account_info(self):
        r = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/account",
            api_instruction="accountQuery",
        )
        return r.json()

    @async_retry(source="Browser", module_str="Enable Auto Lend and Repay", exceptions=Exception)
    async def enable_auto_functions(self):
        payload = {
            "autoLend": True,
            "autoRepayBorrows": True,
            # "autoRealizePnl": True,
        }
        r = await self.session.request(
            method="PATCH",
            url=f"{self.BACKPACK_API}/account",
            json=payload,
            headers=self.build_headers("accountUpdate", payload),
        )
        if r.status_code != 200:
            raise Exception(f'Error: {r.text}')

        acc_info = await self.get_account_info()
        if not acc_info.get("autoLend"):
            raise Exception(f'Failed: {r.text}')

        logger.debug(f'[+] {self.label} | Enabled Auto Lend and Repay')
        return acc_info


    @async_retry(source="Browser", module_str="Get Tickers", exceptions=Exception)
    async def get_tickers(self):
        r = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/tickers",
        )
        prices = {ticker["symbol"].replace("_USDC", ""): float(ticker["lastPrice"]) for ticker in r.json()}
        prices["USDC"] = 1
        return prices


    @async_retry(source="Browser", module_str="Get Balances", exceptions=Exception)
    async def get_balances(self):
        r = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/capital/collateral",
            api_instruction="collateralQuery",
        )
        balances = {
            token_info["symbol"]: float(token_info["totalQuantity"])
            for token_info in r.json()["collateral"]
        }

        r = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/capital",
            api_instruction="balanceQuery",
        )

        for token_name in r.json():
            if balances.get(token_name) is None:
                balances[token_name] = float(r.json()[token_name]["available"])

        return balances


    @async_retry(source="Browser", module_str="Create Order", exceptions=Exception)
    async def create_order(self, payload: dict):
        r = await self.send_request(
            method="POST",
            url=f"{self.BACKPACK_API}/order",
            json=payload,
            api_instruction="orderExecute",
        )
        return r.json()

    @async_retry(source="Browser", module_str="Get Markets", exceptions=Exception)
    async def get_token_decimals(self):
        r = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/markets",
        )
        spot_decimals = {}
        futures_decimals = {}
        for token_info in r.json():
            min_quantity = token_info["filters"]["quantity"]["minQuantity"]
            min_price = token_info["filters"]["price"]["minPrice"]
            min_tick_size = token_info["filters"]["price"].get("tickSize", "0")

            if "." in min_quantity:
                quantity_decimal = len(min_quantity.split('.')[1])
            else:
                quantity_decimal = 0

            if "." in min_price:
                price_decimal = len(min_price.split('.')[1])
            else:
                price_decimal = 0

            if "." in min_tick_size:
                tick_size_decimal = len(min_tick_size.split('.')[1])
            else:
                tick_size_decimal = 0

            token_decimals = {
                    "amount": quantity_decimal,
                    "price": price_decimal,
                    "tick_size": tick_size_decimal
                }
            if token_info["symbol"].endswith("_PERP"):
                futures_decimals[token_info["baseSymbol"]] = token_decimals
            else:
                spot_decimals[token_info["baseSymbol"]] = token_decimals

        for token_name in futures_decimals:
            if spot_decimals.get(token_name) is None:
                spot_decimals[token_name] = futures_decimals[token_name]

        return spot_decimals

    @async_retry(source="Browser", module_str="Get Last Fills", exceptions=Exception)
    async def find_fill_by_id(self, order_id: str, count: int = 0):
        r = await self.send_request(
            method="GET",
            url="https://api.backpack.exchange/wapi/v1/history/fills",
            params={
                "limit": 10,
                "offset": 0,
            },
            api_instruction="fillHistoryQueryAll",
        )
        fills = r.json()
        for fill in fills:
            if fill["orderId"] == order_id:
                return fill
        await asyncio.sleep(2)
        if count < 11:
            return await self.find_fill_by_id(order_id, count=count+1)
        else:
            logger.warning(f'[-] Backpack | Couldnt find order in 25 seconds')
            return None

    @async_retry(source="Browser", module_str="Change Leverage", exceptions=Exception)
    async def change_leverage(self, leverage: int):
        payload = {"leverageLimit": str(leverage)}
        r = await self.session.request(
            method="PATCH",
            url=f"{self.BACKPACK_API}/account",
            json=payload,
            headers=self.build_headers("accountUpdate", payload),
        )
        if r.status_code != 200:
            raise Exception(f'Error: {r.text}')

        acc_info = await self.get_account_info()
        if acc_info.get("leverageLimit") != str(leverage):
            raise Exception(f'Failed: {r.text}')

        logger.debug(f'[+] Backpack | {self.label} | Changed leverage to {leverage}x')
        return acc_info

    @async_retry(source="Browser", module_str="Get Futures Positions", exceptions=Exception)
    async def get_futures_positions(self):
        r = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/position",
            api_instruction="positionQuery",
        )
        return r.json()


    @async_retry(source="Browser", module_str="Get Statistics", exceptions=Exception)
    async def get_stats(self):
        offset = 0
        fills = []

        while True:
            r = await self.send_request(
                method="GET",
                url="https://api.backpack.exchange/wapi/v1/history/fills",
                params={
                    "limit": 1000,
                    "offset": offset,
                },
                api_instruction="fillHistoryQueryAll",
            )
            fills += r.json()
            if len(r.json()) == 1000:
                offset += 1000
            else:
                break

        month_timestamp = int(time() - 60 * 60 * 24 * 30)

        month_volume = round(sum([
            float(fill["price"]) * float(fill["quantity"])
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ]), 2)
        total_volume = round(sum([
            float(fill["price"]) * float(fill["quantity"])
            for fill in fills
        ]), 2)

        month_orders_amounts = len(set([
            fill["orderId"]
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ]))
        total_orders_amounts = len(set([
            fill["orderId"]
            for fill in fills
        ]))

        total_days_amount = len(set([
            datetime.fromisoformat(fill["timestamp"]).strftime('%d-%m-%Y')
            for fill in fills
        ]))
        month_days_amount = len(set([
            datetime.fromisoformat(fill["timestamp"]).strftime('%d-%m-%Y')
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ]))

        return {
            "volume": {"month": month_volume, "total": total_volume},
            "orders": {"month": month_orders_amounts, "total": total_orders_amounts},
            "days": {"month": month_days_amount, "total": total_days_amount},
        }
