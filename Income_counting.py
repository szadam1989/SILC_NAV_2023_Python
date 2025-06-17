import os
import pickle
import pandas as pd
import oracledb

os.chdir("D:/R/Projects/SILC_NAV_2023_Python/Environment/")
file_name = 'SILC'
SILC = None
with open(file_name, "rb") as file:
    SILC = pickle.load(file)

SILC_FEOR2Jegy = SILC.loc[SILC['FEOR08'] != ""]
SILC_FEOR2Jegy = SILC_FEOR2Jegy.sort_values(by = 'FEOR08')
print(len(SILC_FEOR2Jegy)) # 7630
print(len(SILC_FEOR2Jegy.columns)) # 13 oszlop
sorted(set(SILC_FEOR2Jegy['FEOR08'].str.slice(0, 2))) # 42 lekérdezés csak, mert "99" az ismeretlen

Income = pd.DataFrame()
At_least_12_months = pd.DataFrame()
elozoFEORKetjegy = "0"

database = oracledb.makedsn(host = "emerald.ksh.hu", port = "1521", service_name = "emerald.ksh.hu")
conn = oracledb.connect(user = os.getenv("userid"), password = os.getenv("pwd"), dsn = database)
cur = conn.cursor()

for i, x in SILC_FEOR2Jegy.iterrows():
  
  if (SILC_FEOR2Jegy.loc[i, ['FEOR08']].str.slice(0, 2).values[0] == "99"):
    break

  if (elozoFEORKetjegy != SILC_FEOR2Jegy.loc[i, ['FEOR08']].str.slice(0, 2).values[0]):
    
    query = "".join(["select lg.lg_munkaero.dekodol(AAJE) AAJE, LGAA029, to_char(lg.lg_naptar_uj.szulido_adoazbol(lg.lg_munkaero.dekodol(AAJE)), 'YYYY-MM') SZUL_IDO, ML05, nvl(LGAA517, 0) LGAA517, nvl(LGAA520, 0) LGAA520 from LG23.LGAA0_230112_P_V01 where ML62 like '1%' and substr(LGAA029, 1, 2) = '", SILC_FEOR2Jegy.loc[i, ['FEOR08']].str.slice(0, 2).values[0], "'"])
    Income_SzulEvHo = pd.read_sql(query, con = conn)
    Income_SzulEvHo['LGAA517'] = Income_SzulEvHo['LGAA517'].astype(np.float64)
    Income_SzulEvHo['LGAA520'] = Income_SzulEvHo['LGAA520'].astype(np.float64)
    
    elozoFEORKetjegy = SILC_FEOR2Jegy.loc[i, ['FEOR08']].str.slice(0, 2).values[0]
    print(elozoFEORKetjegy)
    
    Income_SzulEvHo_groupby = Income_SzulEvHo.groupby(['AAJE', 'LGAA029']).size().reset_index(name = 'Counts')
    Income_SzulEvHo_groupby = Income_SzulEvHo_groupby.loc[(Income_SzulEvHo_groupby['Counts'] >= 12)]

    At_least_12_months = pd.concat([At_least_12_months, Income_SzulEvHo_groupby], ignore_index = True)

  database_SzulEvHo_FILTER = Income_SzulEvHo.loc[(Income_SzulEvHo['SZUL_IDO'] == "-".join([x['SZEV'], x['SZHO']])) & (Income_SzulEvHo['ML05'] == x['NEME']) & (Income_SzulEvHo['LGAA517'] != 0) & (Income_SzulEvHo['LGAA520'] != 0) ]
  database_SzulEvHo_FILTER = database_SzulEvHo_FILTER.groupby(['AAJE', 'LGAA029', 'SZUL_IDO', 'ML05']).agg({'AAJE': 'first', 'LGAA029': 'first', 'SZUL_IDO': 'first', 'ML05': 'first', 'LGAA517': 'sum', 'LGAA520': 'sum'})
  # database_SzulEvHo_FILTER = database_SzulEvHo_FILTER.groupby(['AAJE', 'LGAA029', 'SZUL_IDO', 'ML05']).agg({'LGAA517': 'sum', 'LGAA520': 'sum'}).reset_index()
   
  if (len(database_SzulEvHo_FILTER) != 0):
    
    database_WITH_SILC_AZON_by_FEOR = database_SzulEvHo_FILTER
    database_WITH_SILC_AZON_by_FEOR.insert(loc = 0, column = 'NEME', value = SILC_FEOR2Jegy.loc[i, 'NEME'])
    database_WITH_SILC_AZON_by_FEOR.insert(loc = 0, column = 'FEOR08', value = SILC_FEOR2Jegy.loc[i, 'FEOR08'])
    database_WITH_SILC_AZON_by_FEOR.insert(loc = 0, column = 'ANYNEV_VIZSGALT', value = SILC_FEOR2Jegy.loc[i, 'ANYNEV_VIZSGALT'])
    database_WITH_SILC_AZON_by_FEOR.insert(loc = 0, column = 'SZNEV_VIZSGALT', value = SILC_FEOR2Jegy.loc[i, 'SZNEV_VIZSGALT'])
    database_WITH_SILC_AZON_by_FEOR.insert(loc = 0, column = 'FIXSZ', value = SILC_FEOR2Jegy.loc[i, 'FIXSZ'])
    Income = pd.concat([Income, database_WITH_SILC_AZON_by_FEOR], ignore_index = True)

cur.close()

conn.close()

# file_name = 'Income'
# with open(file_name, "wb") as file:
#     pickle.dump(Income, file)
# 
# Income.to_parquet("Income.parquet", engine = "pyarrow")

file_name = 'Income.parquet'
Income = pd.read_parquet(file_name, engine = "pyarrow")
print(Income.shape) # 1.666.056 sor és 11 oszlop

# file_name = 'At_least_12_months'
# with open(file_name, "wb") as file:
#     pickle.dump(At_least_12_months, file)
# 
# At_least_12_months.to_parquet("At_least_12_months.parquet", engine = "pyarrow")

file_name = 'At_least_12_months.parquet'
At_least_12_months = pd.read_parquet(file_name, engine = "pyarrow")
print(At_least_12_months.shape) # (2673170, 3)

Incomes_All = Income.iloc[:, 5:11]
Incomes_All = Incomes_All.drop_duplicates()
print(Incomes_All.shape) # (1234710, 6)

Incomes_All = Incomes_All.loc[(Incomes_All['LGAA517'] > 300000)]
print(Incomes_All.shape) # (1181198, 6)

pd.options.mode.chained_assignment = None
Incomes_All['LGAA029_2jegy'] = Incomes_All.LGAA029.str[:2]

mean1 = Incomes_All
mean1 = mean1.groupby(['SZUL_IDO', 'ML05', 'LGAA029_2jegy']).agg({'SZUL_IDO': 'first', 'ML05': 'first', 'LGAA029_2jegy': 'first', 'LGAA517': 'mean', 'LGAA520': 'mean'})
print(mean1.shape) # (6189, 5)
mean1.reset_index(drop = True, inplace = True)
mean1 = mean1.sort_values(by = 'LGAA029_2jegy')

file_save = "Számtani_közép_első_verzió.txt"
mean1.to_csv(file_save, sep = ';', index = False, mode = 'w')

mean2 = Incomes_All
print(mean2.shape) # (1181198, 7)
mean2 = mean2[mean2['AAJE'].isin(At_least_12_months['AAJE'])]
print(mean2.shape) # (797898, 7)

mean2 = mean2.groupby(['ML05', 'SZUL_IDO', 'LGAA029_2jegy']).agg({'LGAA517': 'mean', 'LGAA520': 'mean'}).reset_index()

mean2.reset_index(drop = True, inplace = True)
mean2 = mean2.sort_values(by = 'LGAA029_2jegy')

file_save = "Számtani_közép_JOBB.txt"
mean2.to_csv(file_save, sep = ';', index = False, mode = 'w')
