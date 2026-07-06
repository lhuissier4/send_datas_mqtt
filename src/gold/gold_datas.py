#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
from typing import Optional

import pandas as pd, numpy as np


# In[2]:


df_simule = pd.DataFrame(pd.read_csv(r"../../datas/bronze/dataset_brut.csv"))
df_iot = pd.DataFrame(pd.read_csv(r"../../datas/silver/dataset_iot.csv"))
df_plc = pd.DataFrame(pd.read_csv(r"../../datas/silver/dataset_plc.csv"))


# In[3]:


df_simule.head()


# In[4]:


df_iot.head()


# In[5]:


df_plc.head()


# In[6]:


df_type_machine = df_simule[[
    "type_machine"
]].drop_duplicates(subset=[
        "type_machine",
    ]
)
df_type_machine["id"] = df_type_machine["type_machine"].astype("category").cat.codes + 1
df_type_machine.head(20)


# In[7]:


df_type_metal=df_simule[[
    "type_metal"
]].drop_duplicates(subset=[
        "type_metal",
    ]
)
df_type_metal["id"] = df_type_metal["type_metal"].astype("category").cat.codes + 1
df_type_metal.head(20)


# In[8]:


df_plc = df_plc.merge(
    df_type_metal[["type_metal", "id"]].rename(columns={"id": "id_type_metal"}),
    on="type_metal",
    how="left"
).drop(columns="type_metal")
df_plc.head()


# In[9]:


df_gmao_label = df_simule[[
    "label_gmao"
]].drop_duplicates(subset=[
        "label_gmao",
    ]
)
df_gmao_label["id"] = df_gmao_label["label_gmao"].astype("category").cat.codes + 1
df_gmao_label.head()


# In[10]:


df_production_status = df_simule[[
    "iot_statut_machine"
]].drop_duplicates(subset=[
        "iot_statut_machine",
    ]
)
df_production_status["id"] = df_production_status["iot_statut_machine"].astype("category").cat.codes + 1
df_production_status.head()


# In[11]:


df_plc.head()


# In[12]:


df_plc = df_plc.merge(
    df_production_status[["iot_statut_machine", "id"]].rename(columns={"id": "id_status_production"}),
    on="iot_statut_machine",
    how="left"
).drop(columns="iot_statut_machine")
df_plc.head()


# In[13]:


df_nominale = df_simule[["timestamp", "machine_id", "vitesse_rotation_nominal", "courant_moteur_nominal", "pression_hydraulique_nominal", "statut_nominal", "temp_base_moteur"]]


# In[14]:


from utils import name_csv_file


# In[15]:


from utils import record_future_send_in_jsonl

folder_path = Path("../../datas/gold")
folder_path.mkdir(parents=True, exist_ok=True)
df_type_machine.to_csv(
    name_csv_file(
        folder_path=folder_path,
        filename="type_machine",
        extension=".csv",
        type_dst="postgres"
    ), index=False, encoding='utf-8'
)
df_type_metal.to_csv(
    name_csv_file(
        folder_path=folder_path,
        filename="type_metal",
        extension=".csv",
        type_dst="postgres"
    ), index=False, encoding='utf-8'
)
df_production_status.to_csv(
    name_csv_file(
        folder_path=folder_path,
        filename="production_status",
        extension=".csv",
        type_dst="postgres"
    ), index=False, encoding='utf-8'
)
df_nominale.to_csv(
    name_csv_file(
        folder_path=folder_path,
        filename="nominale_values",
        extension=".csv",
        type_dst="postgres"
    ), index=False, encoding='utf-8'
)


# In[ ]:


df_iot = (
    df_iot.sort_values("timestamp")
      .reset_index(drop=True)
)
df_plc = (
    df_plc.sort_values("timestamp")
      .reset_index(drop=True)
)
record_future_send_in_jsonl(df_iot, df_plc, output_path=r"../../datas/gold/mqtt_iot_plc_send.jsonl")

