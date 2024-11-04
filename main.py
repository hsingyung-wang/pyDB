import importlib
import controllers
from datetime import datetime
import sys

importlib.reload(controllers)
controller = controllers.FuturesDataController()

sys.stdout.reconfigure(encoding='utf-8')

def main():
    update_date = datetime.today().strftime("%Y/%m/%d")
    msg1 = controller.update_twse_data(update_date)
    msg2 = controller.update_taifex_data(update_date)
    print(msg1,flush=True)
    print(msg2,flush=True)


if __name__ == "__main__":
    main()