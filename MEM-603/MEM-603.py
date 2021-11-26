from operator import index

import pandas as pd
import numpy as np

dane = pd.read_csv("dane.csv")
dane["verified_status"] = dane["verified_status"].fillna(0)
dane["sum_of_offers"] = dane["sum_of_offers"].fillna(0)
dane["sum_of_offers_no_clasifieds"] = dane["sum_of_offers_no_clasifieds"].fillna(0)
dane["only_clasifieds"] = np.where((dane["sum_of_offers"] > 0) & (dane["sum_of_offers_no_clasifieds"] == 0), 1,0)

agregat = {"sum_of_offers": "sum",
           "sum_of_offers_no_clasifieds": "sum",
           "seller_id":"count"}

# test na headzie
head = dane.head(1000)
head.to_excel("head.xlsx")
dane_zgrupowane = head.groupby(["year", "verified_status", "only_clasifieds"]).agg(agregat)
dane_zgrupowane.to_excel("zgrupowane_test.xlsx")

# test 2021
df_2021 = dane[dane["year"] == 2021]

# final
dane_zgrupowane = dane.groupby(["year", "verified_status", "only_clasifieds"]).agg(agregat)
dane_zgrupowane = dane_zgrupowane.reset_index()
dane_zgrupowane["verified_status_desc"] = np.where(dane_zgrupowane["verified_status"] == 1, "zwykłe -> firmowe",
                                                   np.where(dane_zgrupowane["verified_status"] == 0, "bez zmian", "firmowe -> zwykłe")
                                                   )
dane_zgrupowane["only_clasifieds_desc"] = np.where(dane_zgrupowane["only_clasifieds"] == 1, "tak", "nie")
dane_zgrupowane = dane_zgrupowane.rename(columns = {"seller_id": "count_seller_id"})
dane_zgrupowane.to_excel("zgrupowane_F.xlsx", index = False)

agregat = {"count_seller_id":"sum"}
dane_procentowe = dane_zgrupowane.groupby(["year","verified_status","verified_status_desc"]).agg(agregat)
dane_procentowe = dane_procentowe \
    .groupby(level = 0)\
    .apply(lambda x: x / float(x.sum()))
dane_procentowe = dane_procentowe.reset_index()
dane_procentowe.to_excel("procentowe_F.xlsx", index = False)

# przykladowy seller
df_10262760 = dane[dane["seller_id"] == 10262760]
# tylko bez clasified
df_only = dane[(dane["only_clasifieds"] == 1) & (dane["year"] == 2021)].drop_duplicates("seller_id")

