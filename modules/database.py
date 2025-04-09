from cryptography.fernet import Fernet
from base64 import urlsafe_b64encode
from random import choice, randint
from time import sleep, time
from os import path, mkdir
from hashlib import md5
import json

from modules.retry import DataBaseError
from modules.utils import logger, WindowName
from settings import (
    SHUFFLE_WALLETS,
    TRADES_COUNT,
    PROXY_TYPE,
    RETRY,
)

from cryptography.fernet import InvalidToken


class DataBase:

    STATUS_SMILES: dict = {
        True: '✅ ',
        False: "❌ ",
        None: "",
        "WARNING": "⚠️ ",
    }

    def __init__(self):

        self.modules_db_name = 'databases/modules.json'
        self.report_db_name = 'databases/report.json'
        self.sell_futures_db_name = 'databases/sell_futures.json'
        self.personal_key = None
        self.window_name = None

        # create db's if not exists
        if not path.isdir(self.modules_db_name.split('/')[0]):
            mkdir(self.modules_db_name.split('/')[0])

        for db_params in [
            {"name": self.modules_db_name, "value": "[]"},
            {"name": self.report_db_name, "value": "{}"},
            {"name": self.sell_futures_db_name, "value": "{}"},
        ]:
            if not path.isfile(db_params["name"]):
                with open(db_params["name"], 'w') as f: f.write(db_params["value"])

        amounts = self.get_amounts()
        logger.info(f'Loaded {amounts["modules_amount"]} modules for {amounts["accs_amount"]} accounts\n')
        if amounts["modules_amount"] == 0:
            pair_count = self.get_pair_count()
            if pair_count:
                logger.warning(f'[!] You have {pair_count} unclosed future positions! Run `2. Futures` to close it')


    def set_password(self):
        if self.personal_key is not None: return

        logger.debug(f'Enter password to encrypt API keys (empty for default):')
        raw_password = input("")

        if not raw_password:
            raw_password = "@karamelniy dumb shit encrypting"
            logger.success(f'[+] Soft | You set empty password for Database\n')
        else:
            print(f'')
        sleep(0.2)

        password = md5(raw_password.encode()).hexdigest().encode()
        self.personal_key = Fernet(urlsafe_b64encode(password))


    def get_password(self):
        if self.personal_key is not None: return

        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        if modules_db:
            test_key = list(modules_db.keys())[0]
        else:
            with open(self.sell_futures_db_name, encoding="utf-8") as f: futures_db = json.load(f)
            if futures_db:
                test_key = futures_db[list(futures_db.keys())[0]]["accounts"][0]["encoded_api_key"]
            else:
                return

        try:
            temp_key = Fernet(urlsafe_b64encode(md5("@karamelniy dumb shit encrypting".encode()).hexdigest().encode()))
            self.decode_pk(pk=test_key, key=temp_key)
            self.personal_key = temp_key
            return
        except InvalidToken: pass

        while True:
            try:
                logger.debug(f'Enter password to decrypt your API keys (empty for default):')
                raw_password = input("")
                password = md5(raw_password.encode()).hexdigest().encode()

                temp_key = Fernet(urlsafe_b64encode(password))
                self.decode_pk(pk=list(modules_db.keys())[0], key=temp_key)
                self.personal_key = temp_key
                logger.success(f'[+] Soft | Access granted!\n')
                return

            except InvalidToken:
                logger.error(f'[-] Soft | Invalid password\n')


    def encode_pk(self, pk: str, key: None | Fernet = None):
        if key is None:
            return self.personal_key.encrypt(pk.encode()).decode()
        return key.encrypt(pk.encode()).decode()


    def decode_pk(self, pk: str, key: None | Fernet = None):
        if key is None:
            return self.personal_key.decrypt(pk).decode()
        return key.decrypt(pk).decode()


    def create_modules(self):
        self.set_password()

        with open('input_data/api_keys.txt') as f: api_keys = f.read().splitlines()
        with open('input_data/proxies.txt') as f: proxies = f.read().splitlines()

        only_keys = []
        labels = []
        for api_key in api_keys:
            splitted_keys = api_key.split(':')
            if len(splitted_keys) != 3:
                raise DataBaseError(f'Invalid API keys format ({api_key}). Valid format `label:api_key:api_secret`')
            only_keys.append(":".join(splitted_keys[-2:]))
            labels.append(splitted_keys[0])

        if PROXY_TYPE == "file":
            with open('input_data/proxies.txt') as f:
                proxies = f.read().splitlines()
            if len(proxies) == 0 or proxies == [""] or proxies == ["http://login:password@ip:port"]:
                logger.error('You will not use proxy')
                proxies = [None for _ in range(len(api_keys))]
            else:
                proxies = list(proxies * (len(api_keys) // len(proxies) + 1))[:len(api_keys)]
        elif PROXY_TYPE == "mobile":
            proxies = ["mobile" for _ in range(len(api_keys))]

        with open(self.report_db_name, 'w') as f: f.write('{}')  # clear report db

        new_modules = {
            self.encode_pk(api_key): {
                "modules": [{"module_name": "backpack", "status": "to_run"} for _ in range(randint(*TRADES_COUNT))],
                "proxy": proxy,
                "label": label,
                "retries": 0,
                "total_pnl": 0,
            }
            for api_key, proxy, label in zip(only_keys, proxies, labels)
        }

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(new_modules, f)

        amounts = self.get_amounts()
        logger.critical(f'Dont Forget To Remove Api Keys from api_keys.txt!')
        logger.info(f'Created Database for {amounts["accs_amount"]} accounts with {amounts["modules_amount"]} modules!\n')


    def get_amounts(self):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        modules_len = sum([len(modules_db[acc]["modules"]) for acc in modules_db])

        for acc in modules_db:
            for index, module in enumerate(modules_db[acc]["modules"]):
                if module["status"] == "failed": modules_db[acc]["modules"][index]["status"] = "to_run"

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)

        if self.window_name == None:
            self.window_name = WindowName(accs_amount=len(modules_db))
        else:
            self.window_name.accs_amount = len(modules_db)

        self.window_name.set_modules(modules_amount=modules_len)

        return {'accs_amount': len(modules_db), 'modules_amount': modules_len}

    def get_accs_left(self):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        return len(set([
            acc
            for acc in modules_db
            for module in modules_db[acc]["modules"]
            if module["status"] == "to_run"
        ]))

    def get_pair_count(self):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        with open(self.sell_futures_db_name, encoding="utf-8") as f: futures_db = json.load(f)
        pair_count = sum([len(modules_db[acc]["modules"]) for acc in modules_db])
        if pair_count % 2:
            pair_count -= 1
        return int(pair_count / 2) + len(futures_db)

    def get_random_module(self, mode: int):
        self.get_password()

        last = False
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        if (
                not modules_db or
                [module["status"] for acc in modules_db for module in modules_db[acc]["modules"]].count('to_run') == 0
        ):
                return 'No more accounts left'

        index = 0
        while True:
            if index == len(modules_db.keys()) - 1: index = 0
            if SHUFFLE_WALLETS: api_key = choice(list(modules_db.keys()))
            else: api_key = list(modules_db.keys())[index]
            module_info = choice(modules_db[api_key]["modules"])
            if module_info["status"] != "to_run":
                index += 1
                continue

            # simulate db
            for module in modules_db[api_key]["modules"]:
                if module["module_name"] == module_info["module_name"] and module["status"] == module_info["status"]:
                    modules_db[api_key]["modules"].remove(module)
                    break

            if mode not in [1, 2] or [module["status"] for module in modules_db[api_key]["modules"]].count('to_run') == 0: # if no modules left for this account
                last = True

            return {
                'api_key': self.decode_pk(pk=api_key),
                'encoded_api_key': api_key,
                'label': modules_db[api_key]["label"],
                'proxy': modules_db[api_key].get("proxy"),
                'module_info': module_info,
                'last': last
            }

    def get_pair_modules(self):
        self.get_password()

        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        accs_left = len(set([acc for acc in modules_db for module in modules_db[acc]["modules"] if module["status"] == "to_run"]))
        if accs_left < 2:
            if accs_left:
                logger.warning(f'[•] Soft | 1 Account left without pair!')
            return 'No more accounts left'

        index = 0
        pair_modules = []
        while True:
            if SHUFFLE_WALLETS: api_key = choice(list(modules_db.keys()))
            else: api_key = list(modules_db.keys())[index]
            account_modules = [module for module in modules_db[api_key]["modules"] if module["status"] == "to_run"]
            if (
                    not account_modules or
                    api_key in [acc["encoded_api_key"] for acc in pair_modules]
            ):
                index += 1
                continue
            module_info = choice(account_modules)
            pair_modules.append({
                'api_key': self.decode_pk(pk=api_key),
                'encoded_api_key': api_key,
                'label': modules_db[api_key]["label"],
                'proxy': modules_db[api_key].get("proxy"),
                'module_info': module_info,
            })

            if len(pair_modules) == 2:
                return pair_modules


    def remove_module(self, module_data: dict):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        for index, module in enumerate(modules_db[module_data["encoded_api_key"]]["modules"]):
            if module["module_name"] == module_data["module_info"]["module_name"] and module["status"] == "to_run":
                if module_data["module_info"]["status"] in [True, "completed"]:
                    self.window_name.add_module()
                    modules_db[module_data["encoded_api_key"]]["modules"].remove(module)
                    modules_db[module_data["encoded_api_key"]]["retries"] = 0
                else:
                    if modules_db[module_data["encoded_api_key"]]["retries"] + 1 >= RETRY:
                        modules_db[module_data["encoded_api_key"]]["retries"] = 0
                        modules_db[module_data["encoded_api_key"]]["modules"][index]["status"] = "failed"
                        self.window_name.add_module()
                    else:
                        modules_db[module_data["encoded_api_key"]]["retries"] += 1
                break

        if [module["status"] for module in modules_db[module_data["encoded_api_key"]]["modules"]].count('to_run') == 0:
            self.report_total_pnl(encoded_key=module_data["encoded_api_key"])
            self.window_name.add_acc()
            send_reports = True
        else:
            send_reports = False

        if not modules_db[module_data["encoded_api_key"]]["modules"]:
            del modules_db[module_data["encoded_api_key"]]

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)
        return send_reports

    def remove_account(self, module_data: dict):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        self.window_name.add_acc()
        if module_data["module_info"]["status"] in [True, "completed"]:
            send_reports = True
            self.report_total_pnl(encoded_key=module_data["encoded_api_key"])
            del modules_db[module_data["encoded_api_key"]]

        else:
            if modules_db[module_data["encoded_api_key"]]["retries"] + 1 >= RETRY:
                modules_db[module_data["encoded_api_key"]]["retries"] = 0
                modules_db[module_data["encoded_api_key"]]["modules"] = [{
                    "module_name": module_data["module_info"]["module_name"],
                    "status": "failed"
                }]
                send_reports = True
                self.report_total_pnl(encoded_key=module_data["encoded_api_key"])
            else:
                modules_db[module_data["encoded_api_key"]]["retries"] += 1
                send_reports = False

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)
        return send_reports

    def remove_pairs(self, pair_modules: list, completed: bool):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        for module_data in pair_modules:
            for index, module in enumerate(modules_db[module_data["encoded_api_key"]]["modules"]):
                if module["module_name"] == module_data["module_info"]["module_name"] and module["status"] == "to_run":
                    if completed:
                        self.window_name.add_module()
                        modules_db[module_data["encoded_api_key"]]["modules"].remove(module)
                        modules_db[module_data["encoded_api_key"]]["retries"] = 0
                    else:
                        if modules_db[module_data["encoded_api_key"]]["retries"] + 1 >= RETRY:
                            modules_db[module_data["encoded_api_key"]]["retries"] = 0
                            modules_db[module_data["encoded_api_key"]]["modules"][index]["status"] = "failed"
                            self.window_name.add_module()
                        else:
                            modules_db[module_data["encoded_api_key"]]["retries"] += 1

                    if not modules_db[module_data["encoded_api_key"]]["modules"]:
                        del modules_db[module_data["encoded_api_key"]]

                    break

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)


    def add_futures_to_sell(self, futures_to_sell: dict, event_name: str):
        with open(self.sell_futures_db_name, encoding="utf-8") as f: futures_db = json.load(f)
        futures_db[event_name] = futures_to_sell
        with open(self.sell_futures_db_name, 'w', encoding="utf-8") as f: json.dump(futures_db, f)

    def get_random_futures_to_sell(self):
        self.get_password()

        with open(self.sell_futures_db_name, encoding="utf-8") as f: futures_db = json.load(f)
        if not futures_db:
            return None

        pair_modules = []
        event_name = choice(list(futures_db.keys()))
        for account_data in futures_db[event_name]["accounts"]:
            pair_modules.append({
                **account_data,
                'api_key': self.decode_pk(pk=account_data["encoded_api_key"])
            })

        return {
            "event_name": event_name,
            "pair_modules": pair_modules,
            "info": futures_db[event_name]["info"]
        }

    def remove_future_to_sell(self, event_name: str):
        with open(self.sell_futures_db_name, encoding="utf-8") as f: futures_db = json.load(f)

        del futures_db[event_name]
        with open(self.sell_futures_db_name, 'w', encoding="utf-8") as f: json.dump(futures_db, f)
        self.window_name.add_acc()


    def add_account_pnl(self, encoded_key: str, bids_spend: float):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        modules_db[encoded_key]["total_pnl"] += bids_spend
        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)

    def report_total_pnl(self, encoded_key: str):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        if modules_db[encoded_key].get('total_pnl'):
            if round(modules_db[encoded_key]['total_pnl'], 3) >= 0:
                total_pnl = f"+{round(modules_db[encoded_key]['total_pnl'], 3)}"
            else:
                total_pnl = f"-{round(abs(modules_db[encoded_key]['total_pnl']), 3)}"
            self.append_report(
                key=encoded_key,
                text=f"\n📈 Bids PNL: {total_pnl}$"
            )


    def append_report(self, key: str, text: str, success: bool | str = None, unique_msg: bool = False):
        with open(self.report_db_name, encoding="utf-8") as f: report_db = json.load(f)

        if not report_db.get(key): report_db[key] = {'texts': [], 'success_rate': [0, 0]}

        if (
                unique_msg and
                report_db[key]["texts"] and
                report_db[key]["texts"][-1] == self.STATUS_SMILES[success] + text
        ):
            return

        report_db[key]["texts"].append(self.STATUS_SMILES[success] + text)
        if success in [False, True]:
            report_db[key]["success_rate"][1] += 1
            if success: report_db[key]["success_rate"][0] += 1

        with open(self.report_db_name, 'w') as f: json.dump(report_db, f)


    def get_account_reports(
            self,
            key: str,
            label: str,
            account_index: str | None = None,
            get_rate: bool = False,
    ):
        with open(self.report_db_name, encoding="utf-8") as f: report_db = json.load(f)

        if account_index is None:
            account_index = f"[{self.window_name.accs_done}/{self.window_name.accs_amount}]"
        elif account_index is False:
            account_index = ""

        header_string = ""
        if account_index:
            header_string = f"{account_index} "
        if label:
            header_string += f"<b>{label}</b>"
        if header_string:
            header_string += "\n\n"

        if report_db.get(key):
            account_reports = report_db[key]
            if get_rate: return f'{account_reports["success_rate"][0]}/{account_reports["success_rate"][1]}'
            del report_db[key]

            with open(self.report_db_name, 'w', encoding="utf-8") as f: json.dump(report_db, f)

            logs_text = '\n'.join(account_reports['texts'])
            tg_text = f'{header_string}{logs_text}'
            if account_reports["success_rate"][1]:
                tg_text += f'\n\nSuccess rate {account_reports["success_rate"][0]}/{account_reports["success_rate"][1]}'

            return tg_text

        else:
            if header_string:
                return f'{header_string}No actions'
