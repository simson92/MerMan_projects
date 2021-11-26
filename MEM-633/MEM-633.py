import pandas as pd

dane = pd.read_csv("dane.csv")

dane.to_excel("podsumowanie.xlsx", index = None)

dane_186864 = dane[dane["nip"] == '186864']

len(dane_186864)
dane_aktywne = dane[dane["activity_0111"] == 1]

len(dane_aktywne)