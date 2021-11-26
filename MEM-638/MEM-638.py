import pandas as pd
import numpy as np

dane = pd.read_csv("dane.csv")
dane["max_log"] = dane["max_log"].fillna('1900-01-01 00:00:00')

dane["logowanie w 6 msc"] = np.where(dane["max_log"] >= '2021-05-01 00:00:00', 1,0)
dane.to_excel("test.xlsx")

agregat = {"nip": "count",
           "f0_": "sum"}

dane_zagregowane = dane.groupby(['oferta_od_2016','logowanie w 6 msc']).agg(agregat)
dane_zagregowane = dane_zagregowane.reset_index()
dane_zagregowane.to_excel("test.xlsx", index = None)