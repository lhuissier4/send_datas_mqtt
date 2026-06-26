from pathlib import Path
from typing import Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd, numpy as np
import os
from gold.utils import *

df_iot:pd.DataFrame = pd.DataFrame(pd.read_csv(r"../datas/gold/mqtt_iot_clean.csv"))
df_plc:pd.DataFrame = pd.DataFrame(pd.read_csv(r"../datas/gold/mqtt_plc_clean.csv"))
df_iot = (
    df_iot.sort_values("timestamp")
      .reset_index(drop=True)
)
df_plc = (
    df_plc.sort_values("timestamp")
      .reset_index(drop=True)
)
list_of_list_of_sensor:list[list[Sensor]] = split_df_by_timestamp_and_create_list_of_sensor(df_iot)
print(list_of_list_of_sensor[0])