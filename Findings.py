import pickle
import os
import pandas as pd

os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
file_name = 'SILC'
SILC = None
with open(file_name, "rb") as file:
    SILC = pickle.load(file)

print(len(SILC.loc[(SILC['Kategoria'] == 1) & (SILC['Talalt'] == 1)])) # 3613, 4154, 4861, 5004, 5195, 5239, 5265, 5281, 5318, 5338, 5382, 5414
print(len(SILC.loc[(SILC['Kategoria'] == 2) & (SILC['Talalt'] == 1)])) # 0
print(len(SILC.loc[(SILC['Kategoria'] == 3) & (SILC['Talalt'] == 1)])) # 2, 3
print(len(SILC.loc[(SILC['Kategoria'] == 4) & (SILC['Talalt'] == 1)])) # 0
print(len(SILC.loc[(SILC['Kategoria'] == 5) & (SILC['Talalt'] == 1)])) # 0
print(len(SILC.loc[(SILC['Kategoria'] == 6) & (SILC['Talalt'] == 1)])) # 146, 172, 187, 191, 192
print(len(SILC.loc[(SILC['Kategoria'] == 7) & (SILC['Talalt'] == 1)])) # 0
print(len(SILC.loc[(SILC['Kategoria'] == 8) & (SILC['Talalt'] == 1)])) # 0
print(len(SILC.loc[(SILC['Kategoria'] == 9) & (SILC['Talalt'] == 1)])) # 0

print(len(SILC.loc[SILC['Talalt'] == 1])) # 5468, 5472, 5489, 5512, 5520, 

print(SILC.loc[SILC['FIXSZ'] == "1808010001"]) # Szőke Attila
print(SILC.loc[SILC['FIXSZ'] == "2886610001"]) # Horváth Ádám

""" SILC.loc[SILC['FIXSZ'] == "1808010001", "Talalt"] = 0
SILC.loc[SILC['FIXSZ'] == "2886610001", "Talalt"] = 0 """ 

print(SILC.loc[SILC['FIXSZ'] == "2525410002"]) # Szabó Andrea
print(SILC.loc[SILC['FIXSZ'] == "2964710001"]) # Seress Zoltán
print(SILC.loc[SILC['FIXSZ'] == "6563110001"]) # Géringer Zoltán

""" SILC.loc[SILC['FIXSZ'] == "2525410002", "Talalt"] = 0
SILC.loc[SILC['FIXSZ'] == "2964710001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6563110001", "Talalt"] = 0 """

print(SILC.loc[SILC['FIXSZ'] == "738910003"]) # Gulyás Ferenc
print(SILC.loc[SILC['FIXSZ'] == "2626010002"]) # Somogyiné Kanta Zsuzsanna
print(SILC.loc[SILC['FIXSZ'] == "6032110002"]) # Merényi János
print(SILC.loc[SILC['FIXSZ'] == "6183310002"]) # Szabó Krisztián
print(SILC.loc[SILC['FIXSZ'] == "6243310003"]) # Kasza Zsuzsanna
print(SILC.loc[SILC['FIXSZ'] == "6341510001"]) # Kiss Sándor
print(SILC.loc[SILC['FIXSZ'] == "6563110001"]) # Géringer Zoltán
print(SILC.loc[SILC['FIXSZ'] == "6629510002"]) # Tóth Lajos
print(SILC.loc[SILC['FIXSZ'] == "7484410001"]) # Kiss Tamás

""" SILC.loc[SILC['FIXSZ'] == "738910003", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "2626010002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6032110002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6183310002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6243310003", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6341510001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6563110001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6629510002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "7484410001", "Talalt"] = 0 
 """
print(SILC.loc[SILC['FIXSZ'] == "1100210002"]) # Egri Jánosné
print(SILC.loc[SILC['FIXSZ'] == "1340110003"]) # Bertalan Bence
print(SILC.loc[SILC['FIXSZ'] == "2314910002"]) # Csizmadia Tiborné
print(SILC.loc[SILC['FIXSZ'] == "2675810002"]) # Mezei Horváth Zsanett
print(SILC.loc[SILC['FIXSZ'] == "2763010001"]) # Korpai János
print(SILC.loc[SILC['FIXSZ'] == "2889110002"]) # Nagy Éva
print(SILC.loc[SILC['FIXSZ'] == "2964710001"]) # Seress Zoltán
print(SILC.loc[SILC['FIXSZ'] == "6501110001"]) # Seregi László
print(SILC.loc[SILC['FIXSZ'] == "6704310001"]) # Karászi Szilárd
print(SILC.loc[SILC['FIXSZ'] == "6770210002"]) # Moldoványi Sándorné
print(SILC.loc[SILC['FIXSZ'] == "7050810002"]) # Vida Krisztina
print(SILC.loc[SILC['FIXSZ'] == "7062710002"]) # Kovács Katalin
print(SILC.loc[SILC['FIXSZ'] == "7624410002"]) # Joó Zsolt
print(SILC.loc[SILC['FIXSZ'] == "7686010001"]) # Horváth Zoltánné
print(SILC.loc[SILC['FIXSZ'] == "7855510001"]) # Kelpi József

""" SILC.loc[SILC['FIXSZ'] == "1100210002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "1340110003", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "2314910002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "2675810002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "2763010001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "2889110002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "2964710001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6501110001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6704310001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "6770210002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "7050810002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "7062710002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "7624410002", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "7686010001", "Talalt"] = 0 
SILC.loc[SILC['FIXSZ'] == "7855510001", "Talalt"] = 0  """











os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
file_name = 'SILC'
with open(file_name, "wb") as file:
    pickle.dump(SILC, file)

# print(SILC.loc[SILC['Kategoria'] == 3])

# print(SILC.loc[(SILC['Kategoria'] == 6) & (SILC['Talalt'] == 0)].to_string())

print(SILC.loc[SILC['SZNAP'] == "00"])

""" print(SILC.loc[SILC['FIXSZ'] == "2457410002", "Talalt"])
print(SILC.loc[SILC['FIXSZ'] == "6880210002", "Talalt"])
print(SILC.loc[SILC['FIXSZ'] == "7079910001", "Talalt"]) """


""" file_name = 'database.parquet'
database = pd.read_parquet(file_name, engine = "pyarrow") """

""" 
print(database.loc[database['VNEVEM'] == ""]) # 608 sor és 8 oszlop

print(database.loc[database['UNEVEM'] == ""]) # 82 sor és 8 oszlop

print(database.loc[database['SZVNEVE'] == ""]) # 762 sor és 8 oszlop

print(database.loc[database['SZUNEVE'] == ""]) # 135 sor és 8 oszlop

print(database.loc[database['AVNEVE'] == ""]) # 3735 sor és 8 oszlop

print(database.loc[database['AUNEVE'] == ""]) # 1314 sor és 8 oszlop """

# print(SILC.loc[SILC['FIXSZ'] == "874110001"])

""" print(SILC.loc[SILC['FIXSZ'] == "2395810001", ["SZNEV_VIZSGALT", "ANYNEV_VIZSGALT"]])
print(database.loc[database['AAJE'] == "8376690884", ["VNEVEM", "UNEVEM", "SZVNEVE", "SZUNEVE", "AVNEVE", "AUNEVE"]]) """


""" print(SILC.loc[SILC['FIXSZ'] == "6701110001", ["SZNEV_VIZSGALT", "ANYNEV_VIZSGALT"]])
print(database.loc[database['AAJE'] == "8403322836", ["VNEVEM", "UNEVEM", "SZVNEVE", "SZUNEVE", "AVNEVE", "AUNEVE"]]) """

""" print(SILC.loc[SILC['FIXSZ'] == "7572110002", ["SZNEV_VIZSGALT", "ANYNEV_VIZSGALT"]])
print(database.loc[database['AAJE'] == "8364403125", ["VNEVEM", "UNEVEM", "SZVNEVE", "SZUNEVE", "AVNEVE", "AUNEVE"]]) """