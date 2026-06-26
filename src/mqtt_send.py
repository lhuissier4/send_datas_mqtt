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
record_future_send_in_jsonl(df_iot, df_plc, output_path=r"../datas/gold/mqtt_iot_plc_send.jsonl")