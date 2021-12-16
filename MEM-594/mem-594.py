import pandas as pd
import numpy as np

# pobieram dane
dane_zgrupowane = pd.read_csv('dane_zgrupowane.csv', low_memory = False)
dane_total = pd.read_csv('dane_zgrupowane_total.csv', low_memory = False)

maincat = dane_zgrupowane[['seller_id', 'maincat']].drop_duplicates()
dane_zgrupowane.drop('maincat', axis = 'columns', inplace = True)
#dane_total.drop('maincat', axis = 'columns', inplace = True)

lista = [dane_zgrupowane, dane_total]
dane = pd.concat(lista)
dane = dane.merge(maincat, how = 'left', on = 'seller_id')

# edycja
values = { 'avg_GMV': 0, 'avg_count_deals': 0, 'avg_count_offers': 0,
           'yesterday': 0,'count_month_sales': 0,'cont_month_offers': 0}
dane = dane.fillna(value=values)

# grupowanie wg kategorii i segmentu w podziale na zweryfikowane/niezweryfikowane
niezweryfikowane = dane[(dane["actual_account_status"] != 'company') & (dane["actual_account_status"] != 'ver')]
zweryfikowane = dane[(dane["actual_account_status"] == 'company') | (dane["actual_account_status"] == 'ver')] #  dane[dane["verified"] == 1]

ls_gr_1 = ["nip", "seller_segment",	"new_meta"]
agr_gr_1 = {'avg_GMV': 'sum',
           	'avg_count_deals': 'sum',
            'avg_count_offers': 'mean'}

df_gr_1 = zweryfikowane.groupby(ls_gr_1).agg(agr_gr_1)
df_gr_1 = df_gr_1.reset_index()
df_gr_1.to_excel("df_gr_1.xlsx", index = None)

# obliczenia nowej segmentacji
dane_mediana = df_gr_1
segmenty = list(df_gr_1["seller_segment"].drop_duplicates().values)
kategorie = list(df_gr_1["new_meta"].drop_duplicates().values)
progi_liczba_transakcji = {'small': 5, 'medium': 10, 'large': 15, 'VIP': 30}

data = []
df = pd.DataFrame(data, columns = ['kategoria','small','medium','large','VIP'])
for kategoria in kategorie:
    df = df.append({'kategoria': kategoria}, ignore_index= True)
df = df.set_index("kategoria")

for segment in segmenty:
    for kategoria in kategorie:
        df_temp = dane_mediana[(dane_mediana["seller_segment"] == segment) &
                               (dane_mediana["avg_count_deals"] >= progi_liczba_transakcji[segment]) &
                               (dane_mediana["new_meta"] == kategoria)]
        df.loc[kategoria, segment] = df_temp["avg_GMV"].quantile(0.25)

df.to_excel("progi_segmentacji_202111.xlsx")

# unpivot
#df.reset_index().melt(id_vars=['kategoria'], var_name = 'segment', value_name = 'kwartyl').to_excel("test progi.xlsx")

# obliczam liczbę kategorii
dane_kategorie = dane_zgrupowane[(dane_zgrupowane["actual_account_status"] != 'company') &
                                                 (dane_zgrupowane["actual_account_status"] != 'ver')]
dane_kategorie = dane_kategorie[['seller_id', 'new_meta']]
agregat = {"new_meta": pd.Series.nunique}
dane_kategorie = dane_kategorie.groupby(["seller_id"]).agg(agregat)
dane_kategorie = dane_kategorie.reset_index()
dane_kategorie.columns = ['seller_id', 'liczba_kategorii']
# dodaje medianę do tabeli
df_segmentyzacja = niezweryfikowane.merge(df, how = "left", left_on = 'new_meta', right_on = 'kategoria')
df_segmentyzacja["new_segment"] = np.where((df_segmentyzacja["avg_GMV"] >= df_segmentyzacja["VIP"]) & (df_segmentyzacja["avg_count_deals"] >= progi_liczba_transakcji["VIP"]), 'VIP',
                                           np.where((df_segmentyzacja["avg_GMV"] >= df_segmentyzacja["large"]) & (df_segmentyzacja["avg_count_deals"] >= progi_liczba_transakcji["large"]) , 'large',
                                                    np.where((df_segmentyzacja["avg_GMV"] >= df_segmentyzacja["medium"]) & (df_segmentyzacja["avg_count_deals"] >= progi_liczba_transakcji["medium"]), 'medium',
                                                             np.where((df_segmentyzacja["avg_GMV"] >= df_segmentyzacja["small"]) & (df_segmentyzacja["avg_count_deals"] >= progi_liczba_transakcji["small"]), 'small',
                                                                      'lack'))))

#df_segmentyzacja = df_segmentyzacja.merge(right = df_prod_stan_pivot[['seller_id', 'new_meta','%nowych']], how = "left", on = ['seller_id','new_meta'])
#df_segmentyzacja = df_segmentyzacja[['seller_id', 'new_segment', 'new_meta', 'count_deals', '%nowych', 'price']]
#df_segmentyzacja.columns = ['id_konta','segment_nowy','kategoria','liczba_transakcji','%nowych','GMV']
df_segmentyzacja = df_segmentyzacja[['seller_id', 'actual_account_status', 'last_date_account_status',
       'company_history', 'new_meta', 'maincat', 'sum_GMV', 'min_date', 'avg_GMV', 'avg_count_deals', 'avg_count_offers',
       'yesterday', 'count_month_sales', 'cont_month_offers',
       'new_segment']]
df_segmentyzacja["avg_GMV"] = df_segmentyzacja["avg_GMV"].round(0)
df_segmentyzacja["avg_count_deals"] = df_segmentyzacja["avg_count_deals"].round(0)
df_segmentyzacja["avg_count_offers"] = df_segmentyzacja["avg_count_offers"].round(0)
df_segmentyzacja = df_segmentyzacja.merge(dane_kategorie, how = 'left', on = 'seller_id')
df_segmentyzacja['last_date_account_status'] = pd.to_datetime(df_segmentyzacja['last_date_account_status'],
                                                              format = '%Y-%m-%d').dt.date
df_segmentyzacja.to_excel("segmentacja_202112.xlsx", index = False)
