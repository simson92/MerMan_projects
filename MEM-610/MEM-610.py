import pandas as pd

dane = pd.read_csv("dane.csv")

dane.to_excel("dane.xlsx")
dane.columns
dane[dane["us_id"] == 102610148]["gmv_30dni"]