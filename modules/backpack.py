from random import choice, random, uniform, randint, shuffle
from decimal import Decimal
from loguru import logger
import asyncio

from .browser import Browser
from .database import DataBase
from .utils import sleeping, cround, make_border
from settings import (
    SLEEP_AFTER_ORDER,
    SLEEP_AFTER_FUTURE,
    TOKENS_TO_TRADE,
    RANDOM_LEVERAGE,
    TRADES_AMOUNT,
    RETRY,
)


class Backpack:

    spot_params: dict = {
        "Bid": "Buy",
        "Ask": "Sell",
    }
    futures_params: dict = {
        "Bid": "Long",
        "Ask": "Short",
    }

    switch_params: dict = {
        "Bid": "Ask",
        "Ask": "Bid",
    }

    def __init__(
            self,
            api_key: str,
            encoded_api_key: str,
            label: str,
            db: DataBase,
            browser: Browser,
            event_name: str | None,
            order_data: dict | None,
    ):
        self.db = db
        self.label = label
        self.browser = browser
        self.api_key = api_key
        self.order_data = order_data
        self.encoded_api_key = encoded_api_key
        self.event_name = event_name or encoded_api_key

        self.mode = None
        self.account_info = None
        self.balances = None
        self.prices = None
        self.token_decimals = {}

        self.bids_history = {}


    async def run_mode(self, mode: int, last: bool):
        self.mode = mode
        await self.login()

        if mode in [1, 3]:
            self.prices, self.balances, self.token_decimals = await asyncio.gather(
                self.browser.get_tickers(),
                self.browser.get_balances(),
                self.browser.get_token_decimals(),
            )

        if mode == 1:
            status = await self.trade()

        elif mode == 3:
            status = await self.sell_all()

        if mode == 4:
            status = await self.parse_stats()

        return status


    async def login(self):
        self.account_info = await self.browser.get_account_info()

        if not self.account_info.get("autoLend"):
            self.account_info = await self.browser.enable_auto_functions()


    async def trade(self):
        random_token = None
        max_funded_token, existing_tokens = self.find_tokens()

        if (
                TRADES_AMOUNT["amount"] != [0, 0] and
                self.balances["USDC"] < TRADES_AMOUNT["amount"][0]
            ) or (
                TRADES_AMOUNT["amount"] == [0, 0] and
                self.balances["USDC"] < 1
        ):
            if not max_funded_token:
                raise Exception(f'No tokens to trade: {self.balances["USDC"]} USDC')
            else:
                logger.debug(f'[•] Backpack | Not enough USDC, selling {random_token}')
                random_token = max_funded_token

        elif max_funded_token and random() < 0.2:
            random_token = max_funded_token
            logger.debug(f'[•] Backpack | Randomly selling {random_token}')

        if random_token:
            await self.sell_token(token_name=random_token, all_balance=True)
            sleeping(SLEEP_AFTER_ORDER)

        random_token = choice(existing_tokens)

        if not await self.buy_token(token_name=random_token):
            return False

        sleeping(SLEEP_AFTER_ORDER)

        return await self.sell_token(token_name=random_token)

    def find_tokens(self):
        funded_tokens = {}
        valid_tokens = ", ".join([ticker for ticker in self.prices if not ticker.endswith("_PERP")])

        for token_name in TOKENS_TO_TRADE:
            if token_name not in self.prices:
                logger.warning(f'[-] Backpack | Invalid token to trade "{token_name}"\nValid tokens: {valid_tokens}')
                continue

            elif token_name not in self.balances:
                continue

            token_usd_balance = self.prices[token_name] * cround(self.balances[token_name], self.token_decimals[token_name]["amount"])
            if token_usd_balance > 1: funded_tokens[token_name] = token_usd_balance

        if funded_tokens:
            max_funded_token = max(funded_tokens, key=funded_tokens.get)
        else:
            max_funded_token = None

        return max_funded_token, [token for token in TOKENS_TO_TRADE if token in self.prices]

    async def buy_token(self, token_name: str, all_balance: bool = False):
        if all_balance:
            self.balances = await self.browser.get_balances()
            usdc_amount = self.balances["USDC"]

        elif TRADES_AMOUNT["amount"] != [0, 0]:
            usdc_amounts = TRADES_AMOUNT["amount"].copy()
            if usdc_amounts[0] > self.balances["USDC"]:
                raise Exception(
                    f'You want to swap for at least {usdc_amounts[0]}$ but have only'
                    f' {cround(self.balances["USDC"], 2)} USDC'
                )
            elif usdc_amounts[1] > self.balances["USDC"]:
                usdc_amounts[1] = self.balances["USDC"]
            usdc_amount = uniform(*usdc_amounts)

        else:
            percent = uniform(*TRADES_AMOUNT["percent"]) / 100
            usdc_amount = self.balances["USDC"] * percent

        amount = usdc_amount / self.prices[token_name]

        return await self.create_spot_order(
            side="Bid",
            token_name=token_name,
            amount=cround(amount, self.token_decimals[token_name]["amount"]),
        )

    async def sell_token(self, token_name: str, all_balance: bool = False):
        if all_balance:
            self.balances = await self.browser.get_balances()
            amount = self.balances[token_name]

        else:
            percent = uniform(*TRADES_AMOUNT["percent_back"]) / 100
            amount = self.balances[token_name] * percent

        return await self.create_spot_order(
            side="Ask",
            token_name=token_name,
            amount=cround(amount, self.token_decimals[token_name]["amount"]),
        )

    async def create_spot_order(
            self,
            side: str,
            token_name: str,
            amount: float,
            retry: int = 0,
    ):
        self.prices = await self.browser.get_tickers()
        last_price = self.prices[token_name]
        if side == "Bid":
            price = cround(last_price * 1.008, self.token_decimals[token_name]["price"])
        elif side == "Ask":
            price = cround(last_price * 0.992, self.token_decimals[token_name]["price"])
        str_amount = str(round(Decimal(amount), self.token_decimals[token_name]["amount"]))

        payload = {
            "side": side,
            "symbol": f"{token_name}_USDC",
            "orderType": "Limit",
            "timeInForce": "IOC",
            "quantity": str_amount,
            "price": str(price),
            "autoBorrow": False,
            "autoBorrowRepay": False,
            "autoLendRedeem": True,
            "autoLend": False
        }
        order_resp = await self.browser.create_order(payload)

        order_price = last_price
        if order_resp.get("id"):
            current_fill = await self.browser.find_fill_by_id(order_resp["id"])
            if current_fill:
                order_price = float(current_fill["price"])

        action_name = f'<blue>{self.spot_params[side]}</blue>'
        first_token = f"{str_amount} {token_name}"
        second_token = f"{cround(amount * order_price, 2)} USDC"

        if side == "Bid":
            formatted_tokens = f"<blue>{first_token}</blue> for {second_token}"
        else:
            formatted_tokens = f"{first_token} for <blue>{second_token}</blue>"

        if order_resp.get("createdAt"):
            if order_resp.get("status") == "Filled":
                bids_spend_str = ""
                tg_status, tg_report = True, f"{self.spot_params[side].lower()} {first_token} for {second_token} (${order_price})"

                if TRADES_AMOUNT["percent_back"][0] >= 99 and TRADES_AMOUNT["percent_back"][1] >= 99:
                    if side == "Bid":
                        self.bids_history["buy"] = {"amount": amount, "price": order_price}

                    elif side == "Ask" and self.bids_history.get("buy"):
                        min_amount = min(self.bids_history["buy"]["amount"], amount)
                        bids_spend = (order_price - self.bids_history["buy"]["price"]) * min_amount
                        self.db.add_account_pnl(encoded_key=self.encoded_api_key, bids_spend=bids_spend)
                        if bids_spend >= 0: bids_spend_str = f" | Profit +{round(bids_spend, 3)}$"
                        else: bids_spend_str = f" | Profit -{round(abs(bids_spend), 3)}$"

                logger.opt(colors=True).info(f'[+] Backpack | {action_name} {formatted_tokens} (${order_price}){bids_spend_str}')

            elif order_resp.get("status") == "New":
                logger.opt(colors=True).debug(
                    f'[•] Backpack | Open {action_name} <white>limit order</white> {formatted_tokens} (${order_price})'
                )
                tg_status, tg_report = "WARNING", f"open {self.spot_params[side].lower()} limit order {first_token} for {second_token} (${order_price})"

            else:
                logger.opt(colors=True).error(
                    f'[-] Backpack | Failed to {self.spot_params[side].lower()} {str_amount} {token_name} for {order_price} '
                    f'(status {order_resp.get("status")})'
                )
                tg_status, tg_report = False, f"{self.spot_params[side].lower()} {first_token} for {second_token} (${order_price})"

        else:
            if order_resp.get("message"):
                error_text = order_resp["message"]
            else:
                error_text = order_resp

            logger.opt(colors=True).warning(
                f'[-] Backpack | Failed to {action_name} {formatted_tokens} (${order_price}): '
                f'Unexpected response: {error_text}'
            )
            tg_status, tg_report = False, f"{self.spot_params[side].lower()} {first_token} for {second_token} (${order_price})"

        self.db.append_report(
            key=self.event_name,
            text=tg_report,
            success=tg_status,
            unique_msg=True
        )

        self.balances = await self.browser.get_balances()

        if order_resp.get("status") in ["Filled", "New"]:
            return True
        else:
            if retry < RETRY:
                return await self.create_spot_order(side=side, token_name=token_name, amount=amount, retry=retry+1)
            else:
                return False


    async def create_futures_order(
            self,
            side: str,
            token_name: str,
            usdc_amount: float,
            token_amount: float,
            need_label: bool,
            leverage: int,
            retry: int = 0,
    ):
        if need_label:
            str_label = f"{self.label} | "
        else:
            str_label = ""

        if usdc_amount:
            payload = {
                "orderType": "Market",
                "quoteQuantity": str(usdc_amount),
                "side": side,
                "symbol": f"{token_name}_USDC_PERP",
                "reduceOnly": False
            }
            raw_action_name = self.futures_params[side]
            if leverage:
                leverage_str = f" | Leverage {leverage}x"
            else:
                leverage_str = ""

        elif token_amount:
            str_token_amount = str(round(Decimal(token_amount), self.token_decimals[token_name]["amount"]))
            payload = {
                "orderType": "Market",
                "quantity": str_token_amount,
                "side": side,
                "symbol": f"{token_name}_USDC_PERP",
                "reduceOnly": True
            }
            leverage_str = ""
            raw_action_name = f"Sell {self.futures_params[self.switch_params[side]]}"

        else:
            raise Exception("One of `usdc_amount` or `token_amount` must be filled")

        action_name = f'<blue>{raw_action_name}</blue>'
        order_resp = await self.browser.create_order(payload)

        if order_resp.get("status") and order_resp.get("status") == "Filled":
            token_amount = float(order_resp['executedQuantity'])
            str_token_amount = str(round(Decimal(token_amount), self.token_decimals[token_name]["amount"]))
            usdc_amount = float(order_resp['executedQuoteQuantity'])
            first_token = f"<blue>{str_token_amount} {token_name}</blue>"
            second_token = f"{round(usdc_amount, 2)} USDC"
            tokens_str = f"{first_token} for {second_token}"

            order_price = 0
            if order_resp.get("id"):
                current_fill = await self.browser.find_fill_by_id(order_resp["id"])
                if current_fill:
                    order_price = float(current_fill["price"])

            bids_spend_str = ""
            tg_status, tg_report = True, f"{raw_action_name.lower()} {str_token_amount} {token_name} for {second_token} (${order_price}){leverage_str.lower()}"
            logger.opt(colors=True).info(f'[+] Backpack | {str_label}{action_name} {tokens_str} (${order_price}){leverage_str}{bids_spend_str}')

            self.bids_history = {
                "amount": token_amount,
                "price": order_price,
                "usdc": usdc_amount,
                "order_id": order_resp["id"],
                "token_name": token_name,
                "side": side,
            }

        else:
            tokens_str = f"<blue>{token_name}</blue> for USDC"

            if order_resp.get("message"):
                error_text = order_resp["message"]
            else:
                error_text = order_resp

            logger.opt(colors=True).warning(f'[-] Backpack | {str_label}Failed to {action_name} {tokens_str} {leverage_str} | Unexpected response: {error_text}')
            tg_status, tg_report = False, f"{raw_action_name.lower()} {token_name} for USDC {leverage_str}"

        tg_text = f"<i>{self.label}</i>\n{tg_report}\n" if need_label else tg_report
        self.db.append_report(
            key=self.event_name,
            text=tg_text,
            success=tg_status,
            unique_msg=True
        )

        if tg_status:
            return self.bids_history
        else:
            if retry < RETRY:
                return await self.create_futures_order(
                    side=side,
                    token_name=token_name,
                    usdc_amount=usdc_amount,
                    token_amount=token_amount,
                    need_label=need_label,
                    leverage=leverage,
                    retry=retry+1
                )
            else:
                return False


    async def sell_all(self):
        futures_positions = await self.browser.get_futures_positions()
        tokens_to_sell = [
            token_name
            for token_name in self.balances
            if (
                    token_name in self.prices and
                    token_name in self.token_decimals and
                    token_name != "USDC" and not token_name.endswith("_PERP") and
                    self.prices[token_name] * cround(self.balances[token_name], self.token_decimals[token_name]["amount"]) > 1
            )
        ]

        if not tokens_to_sell and not futures_positions:
            logger.info(f'[•] Backpack | No tokens to sell found')
            self.db.append_report(
                key=self.event_name,
                text="no tokens to sell found",
                success=True,
            )
            return True

        for token_name in tokens_to_sell:
            position_amount = cround(self.balances[token_name], self.token_decimals[token_name]["amount"])
            if not position_amount:
                logger.warning(f'[-] Backpack | Low {token_name} balance ({self.balances[token_name]}) to sell')
                self.db.append_report(
                    key=self.event_name,
                    text=f"low {token_name} balance to sell",
                    success=False,
                )
                continue

            await self.sell_token(token_name, all_balance=True)

            if token_name != tokens_to_sell[-1] or futures_positions:
                sleeping(SLEEP_AFTER_ORDER)

        for position in futures_positions:
            token_name = position["symbol"].removesuffix("_USDC_PERP")
            side = "Bid" if position["netQuantity"].startswith("-") else "Ask"
            position_amount = cround(
                float(position["netExposureQuantity"]),
                self.token_decimals[token_name]["amount"]
            )
            if not position_amount:
                logger.warning(f'[-] Backpack | Low {token_name} balance ({position["netExposureQuantity"]}) '
                             f'to close {self.futures_params[self.switch_params[side]].lower()}')
                self.db.append_report(
                    key=self.event_name,
                    text=f"low {token_name} balance to close {self.futures_params[self.switch_params[side]].lower()}",
                    success=False,
                )
                continue

            await self.create_futures_order(
                side=side,  # reversed
                token_name=token_name,
                usdc_amount=0,
                token_amount=float(position["netExposureQuantity"]),
                need_label=False,
                leverage=0,
            )
            if position != futures_positions[-1]:
                sleeping(SLEEP_AFTER_ORDER)

        return True


    async def change_leverage(self, new_leverage: int):
        if self.account_info.get('leverageLimit') != str(new_leverage):
            self.account_info = await self.browser.change_leverage(new_leverage)
            return True
        return False


    async def parse_stats(self):
        stats, self.balances, self.prices = await asyncio.gather(
            self.browser.get_stats(),
            self.browser.get_balances(),
            self.browser.get_tickers(),
        )

        total_usd_balance = round(sum([
            self.balances[token_name] * self.prices[token_name]
            for token_name in self.balances
            if token_name in self.prices
        ]), 2)
        usdc_balance = round(self.balances["USDC"], 2) if self.balances.get("USDC") else 0

        log_text = {
            "Month Volume": f'{stats["volume"]["month"]}$',
            "Month Orders": stats["orders"]["month"],
            "Month Unique Days": stats["days"]["month"],
            "Total Volume": f'{stats["volume"]["total"]}$',
            "Total Orders": stats["orders"]["total"],
            "Total Unique Days": stats["days"]["total"],
            "USDC Balance": usdc_balance,
            "Total Balance": f"{total_usd_balance}$",
        }
        logger.success(f"Account statistics:\n{make_border(log_text)}")

        tg_log = f"""<b>Month Statistics</b>:
💵 Volume: {stats['volume']['month']}$
💼 Orders: {stats['orders']['month']}
📅 Unique Days: {stats['days']['month']}

<b>Total Statistics</b>:
💵 Volume: {stats['volume']['total']}$
💼 Orders: {stats['orders']['total']}
📅 Unique Days: {stats['days']['total']}

💸 USDC Balance: {usdc_balance}
💰 Total Balance: {total_usd_balance}$"""
        self.db.append_report(
            key=self.event_name,
            text=tg_log
        )

        return True


class FuturesPair:

    def __init__(self, account1: Backpack, account2: Backpack):
        self.account1 = account1
        self.account2 = account2


    async def run(self, buy: bool, **kwargs):
        await asyncio.gather(
            self.account1.login(),
            self.account2.login(),
        )

        self.account1.balances, self.account2.balances, token_decimals = await asyncio.gather(
            self.account1.browser.get_balances(),
            self.account2.browser.get_balances(),
            self.account1.browser.get_token_decimals(),
        )
        self.account1.token_decimals, self.account2.token_decimals = token_decimals, token_decimals


        if buy:
            return await self.open_futures(**kwargs)
        else:
            return await self.close_futures(**kwargs)


    async def open_futures(self, token_name: str, **kwargs):
        if TRADES_AMOUNT["amount"] != [0, 0]:
            amounts_range = TRADES_AMOUNT["amount"].copy()
            for account in [self.account1, self.account2]:
                if account.balances["USDC"] < amounts_range[0]:
                    raise Exception(f'{account.label} has low USDC balance: {round(account.balances["USDC"], 2)}')
                elif account.balances["USDC"] < amounts_range[1]:
                    amounts_range[1] = account.balances["USDC"]
            bid_amount = uniform(*amounts_range)

        else:
            min_balance = min(self.account1.balances["USDC"], self.account2.balances["USDC"])
            random_percent = uniform(*TRADES_AMOUNT["percent"])
            bid_amount = min_balance * random_percent / 100

            if bid_amount < 1.5:
                if self.account1.balances["USDC"] == min_balance: low_acc = self.account1
                else: low_acc = self.account2
                raise Exception(f'{low_acc} has low USDC balance: {round(min_balance, 2)} (with {round(random_percent, 2)}%)')

        bought_futures = []
        sides = ["Bid", "Ask"]
        random_leverage = randint(*RANDOM_LEVERAGE)
        leveraged_bid_amount = bid_amount * random_leverage
        for account in [self.account1, self.account2]:
            order_data = await self.create_future_order(
                account=account,
                leverage=random_leverage,
                token_name=token_name,
                side=sides.pop(randint(0, len(sides) - 1)),
                usdc_amount=leveraged_bid_amount,
            )
            if not order_data:
                raise Exception(f'Future order failed, cant continue working')
            else:
                bought_futures.append(order_data)

                if account == self.account1:
                    sleeping(SLEEP_AFTER_FUTURE)

        # calculate BUY profit
        buy_profit = round(sum([
            acc["order_data"]["usdc"] if acc["order_data"]["side"] == "Ask"
            else -acc["order_data"]["usdc"]
            for acc in bought_futures
        ]), 2)
        profit_str = f"+{buy_profit}" if buy_profit >= 0 else f"{buy_profit}"
        logger.info(f'[•] Backpack | Future pair buy difference: {profit_str}$')
        self.account1.db.append_report(
            key=self.account1.event_name,
            text=f"{'📈' if buy_profit >= 0 else '📉'} buy difference {profit_str}$"
        )

        pair_index = self.account1.db.window_name.get_next_pairs_index()
        futures_to_sell = {
            "accounts": bought_futures,
            "info": {
                "token_name": token_name,
                "pair_index": pair_index,
                "buy_profit": buy_profit,
            }
        }
        self.account1.db.add_futures_to_sell(futures_to_sell, self.account1.event_name)

        return True, pair_index

    async def close_futures(self, **kwargs):
        random_percent = uniform(*TRADES_AMOUNT["percent_back"]) / 100
        accounts_list = [self.account1, self.account2]
        shuffle(accounts_list)

        total_profit = 0
        for account in accounts_list:
            order_data = await self.create_future_order(
                account=account,
                token_name=account.order_data["token_name"],
                side=account.switch_params[account.order_data["side"]],
                token_amount=account.order_data["amount"] * random_percent,
            )
            if not order_data:
                raise Exception(f'Future order failed, cant continue working')
            else:
                total_profit += order_data["order_data"]["usdc"] - account.order_data["usdc"]
                if account == accounts_list[0]:
                    sleeping(SLEEP_AFTER_FUTURE)

        profit_str = f"+{round(total_profit, 2)}" if total_profit >= 0 else f"{round(total_profit, 2)}"
        logger.info(f'[•] Backpack | Futures profit: {profit_str}$')
        self.account1.db.append_report(
            key=self.account1.event_name,
            text=f"{'📈' if total_profit >= 0 else '📉'} <b>futures profit {profit_str}$</b>"
        )

        return True, None


    async def create_future_order(
            self,
            account: Backpack,
            token_name: str,
            side: str,
            usdc_amount: float = 0,
            token_amount: float = 0,
            leverage: int = 0,
    ):
        if leverage and await account.change_leverage(leverage):
            await asyncio.sleep(randint(3,8))

        order_data = await account.create_futures_order(
            side=side,
            token_name=token_name,
            usdc_amount=cround(usdc_amount, account.token_decimals[token_name]["tick_size"]),
            token_amount=cround(token_amount, account.token_decimals[token_name]["amount"]),
            need_label=True,
            leverage=leverage,
        )
        if not order_data:
            return False

        return {
            'encoded_api_key': account.encoded_api_key,
            'label': account.label,
            'proxy': account.browser.proxy,
            'order_data': order_data
        }
