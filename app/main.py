import sys
import yaml
import time
import datetime

from measure import *
from stattable import *
from pathlib import Path




# -------------------------------------------

if __name__ == "__main__":

    args = sys.argv

    setting_path = args[1]
    log_dir = args[2]



    with open(setting_path) as file:
        settings = yaml.safe_load(file)

    logs = []
    stattable = StatTable()

    for setting in settings:


        log = {
            "id": setting["id"],
            "description": setting["description"]
            }
        
        dimensions = [dimension["name"] for dimension in setting["dimension"]]

        measures = {measure["name"]:[measure_freq, measure_sum, measure_var, measure_std, measure_mean] for measure in setting["measure"]}

        src = Path(setting["src"])

        ext = src.suffix


        time_start = time.time()

        if ext == ".csv":
            st = stattable.create_using_csv(
                str(src), dimensions, measures, chunksize = 500000)

        elif ext == ".parquet":
            st = stattable.create_using_parquet(
                str(src), dimensions, measures)

        st.to_csv(setting["dest"], index = False)

        time_end = time.time()
        tim = time_end- time_start

        log["result"] = f'{tim}秒'

        logs.append(log)


    log_name = datetime.datetime.now().strftime('log_%Y%m%d_%H%M%S.csv')
    pd.DataFrame(logs).to_csv(f"{log_dir}/{log_name}", index = False)

