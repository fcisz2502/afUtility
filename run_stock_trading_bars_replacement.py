from afUtility.get_jq_data import replace_spike6_trading_bars, replace_spike5_trading_bars
import threading
from time import sleep
from datetime import datetime, time
import json
import os


# ----------------------------------------------------------------------------------------------------------------------
def chileProcess():
    with open(
            os.path.join(
                os.path.split(os.path.realpath(__file__))[0],
                'spike_trading_monitor_settings.json'
            ),
            'r'
    ) as f:
        trading_stocks = json.load(f)
        spike5_stocks = trading_stocks['spike5']
        spike6_stocks = trading_stocks['spike6']

    replace_spike6_trading_bars(spike6_stocks)
    replace_spike5_trading_bars(spike5_stocks)


# ----------------------------------------------------------------------------------------------------------------------
def daemonProcess():
    has_run_data_replacement = False
    while True:
        if datetime.now().weekday() < 5:
            if time(11, 28) < datetime.now().time() < time(11, 30) or time(14, 58) < datetime.now().time() < time(15):
                has_run_data_replacement = False
                print('has run data replace is %s.' % has_run_data_replacement)

            elif time(11, 45) < datetime.now().time() < time(11, 50) \
                    or time(15, 25) < datetime.now().time() < time(15, 30):
                if not has_run_data_replacement:
                    threading.Thread(
                        target=chileProcess
                    ).start()
                    has_run_data_replacement = True
                print('has run data replace is %s.' % has_run_data_replacement)
            else:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            sleep(60)
        else:
            print('Weekend')
            sleep(60*60)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # chileProcess()
    daemonProcess()
