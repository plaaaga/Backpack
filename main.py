from random import choice, random
from os import name as os_name
from time import time
import asyncio

from modules.utils import sleeping, logger, sleep, choose_mode
from modules.retry import DataBaseError
from modules import *
import settings


def initialize_account(module_data: dict, event_name: str | None = None):
    browser = Browser(
        encoded_api_key=module_data["encoded_api_key"],
        api_key=module_data["api_key"],
        db=db,
        proxy=module_data["proxy"],
        label=module_data["label"],
    )
    return Backpack(
        api_key=module_data["api_key"],
        encoded_api_key=module_data["encoded_api_key"],
        label=module_data["label"],
        order_data=module_data.get("order_data"),
        browser=browser,
        db=db,
        event_name=event_name,
    )


async def run_modules(mode: int):
    while True:
        module_data = None
        print('')
        try:
            module_data = db.get_random_module(mode)

            if module_data == 'No more accounts left':
                logger.success(f'All accounts done.')
                return 'Ended'

            backpack = initialize_account(module_data)
            module_data["module_info"]["status"] = await backpack.run_mode(mode=mode, last=module_data['last'])

        except Exception as err:
            logger.error(f'[-] Web3 | Account error: {err}')
            db.append_report(key=backpack.encoded_api_key, text=str(err), success=False)

        finally:
            if type(module_data) == dict:
                if mode == 1:
                    send_reports = db.remove_module(module_data=module_data)
                else:
                    send_reports = db.remove_account(module_data=module_data)

                if send_reports and module_data['last']:
                    reports = db.get_account_reports(key=backpack.encoded_api_key, label=backpack.label)
                    TgReport().send_log(logs=reports)

                if module_data["module_info"]["status"] is True: sleeping(settings.SLEEP_AFTER_ACC)
                else: sleeping(10)


async def run_many_accs():
    db.window_name.set_accs(accs_amount=db.get_pair_count())
    while True:
        pair_modules = None
        futures_to_sell = None
        pair_index = False
        completed = False
        random_token = choice(settings.TOKENS_TO_TRADE)
        event_name = f"{random_token}_{int(time() * 1e3)}"

        print('')
        try:
            futures_to_sell = db.get_random_futures_to_sell()
            if futures_to_sell and (
                    settings.SELL_CHANCE > random() * 100 or
                    db.get_accs_left() < 2
            ):
                pair_modules = futures_to_sell["pair_modules"]
                event_name = futures_to_sell["event_name"]

            else:
                futures_to_sell = None
                pair_modules = db.get_pair_modules()
                if pair_modules == 'No more accounts left':
                    logger.success(f'All accounts done.')
                    return 'Ended'

            backpack1 = initialize_account(pair_modules[0], event_name)
            backpack2 = initialize_account(pair_modules[1], event_name)

            completed, pair_index = await FuturesPair(backpack1, backpack2).run(
                buy=not futures_to_sell,
                token_name=random_token
            )

        except Exception as err:
            logger.error(f'[-] Web3 | Futures error: {err}')
            db.append_report(key=event_name, text=str(err), success=False)

        finally:
            if type(pair_modules) == list:
                if futures_to_sell:
                    if completed:
                        db.remove_future_to_sell(event_name=event_name)
                        label = f"Sell {futures_to_sell['info']['token_name']}"
                        pair_index = futures_to_sell["info"]["pair_index"]
                    else:
                        label = ""
                else:
                    db.remove_pairs(pair_modules=pair_modules, completed=completed)
                    if pair_index:
                        label = f"Buy {random_token}"
                    else:
                        label = ""

                reports = db.get_account_reports(key=event_name, label=label, account_index=pair_index)
                if reports:
                    TgReport().send_log(logs=reports)

                if completed: sleeping(settings.SLEEP_AFTER_ACC)
                else: sleeping(10)


if __name__ == '__main__':
    if os_name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        db = DataBase()

        while True:
            mode = choose_mode()

            match mode:
                case None: break

                case 'Delete and create new':
                    db.create_modules()

                case 1 | 3 | 4:
                    if asyncio.run(run_modules(mode=mode)) == 'Ended': break
                    print('')

                case 2:
                    if asyncio.run(run_many_accs()) == 'Ended': break
                    print('')

        sleep(0.1)
        input('\n > Exit\n')

    except DataBaseError as e:
        logger.error(f'[-] Database | {e}')

    except KeyboardInterrupt:
        pass

    finally:
        logger.info('[•] Soft | Closed')
