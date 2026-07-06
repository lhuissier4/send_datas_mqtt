#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd, numpy as np


# In[2]:


df_simule = pd.read_csv(r"../../datas/bronze/dataset_brut.csv")


# In[3]:


df_simule.head()


# In[4]:


df_simule.info()


# In[5]:


df_clean_iot = df_simule.drop(columns=[
    "age_jours",
    "age_virtuel_jours",
    "label_gmao",
    "RUL_jours",
    "secteur",
    "type_machine",
    "vitesse_rotation_nominal",
    "courant_moteur_nominal",
    "pression_hydraulique_nominal",
    "statut_nominal",
    "type_metal",
    "temp_base_moteur",
    "iot_statut_machine",
    "iot_vibration_rms"
])
df_clean_plc = df_simule[[
        "machine_id",
        "timestamp",
        "iot_statut_machine",
        "type_metal",
]]


# In[6]:


df_clean_iot["timestamp"] = pd.to_datetime(
    df_clean_iot["timestamp"],
    format="%Y-%m-%d %H:%M:%S"
)
df_clean_plc["timestamp"] = pd.to_datetime(
    df_clean_plc["timestamp"],
    format="%Y-%m-%d %H:%M:%S"
)
df_clean_iot.info()


# In[7]:


df_clean_iot = (
    df_clean_iot.sort_values("timestamp")
      .reset_index(drop=True)
)
df_clean_plc = (
    df_clean_plc.sort_values("timestamp")
      .reset_index(drop=True)
)


# In[8]:


pd.set_option("display.max_colwidth", None)
df_clean_iot.head(50)


# In[9]:


df_clean_plc.head(50)


# In[10]:


df_clean_iot.to_csv(r"../../datas/silver/dataset_iot.csv", index=False, encoding='utf-8')
df_clean_plc.to_csv(r"../../datas/silver/dataset_plc.csv", index=False, encoding='utf-8')

