import pandas as pd
from fuzzywuzzy import fuzz
import pickle
import os

os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
file_name = 'SILC'
SILC = None
with open(file_name, "rb") as file:
    SILC = pickle.load(file)

file_name = 'database.parquet'
database = pd.read_parquet(file_name, engine = "pyarrow")

os.chdir("D:/Python/Projects/SILC_NAV_2023/Result/")
file_save = 'Talalt_Parok_1._kat_Token_Set_Ratio_First_2_names_90_PYTHON.txt'
if (os.path.exists(file_save)):
    os.remove(file_save)

for index, x in SILC.iterrows():

    if(x['Kategoria'] != 1 or x['Talalt'] != 0):
        continue
    
    SILC_Name = x['SZNEV_VIZSGALT'].split(" ")
    SILC_Mother_Name = x['ANYNEV_VIZSGALT'].split(" ")

    database_Subset = database.loc[database['SZUL_DAT'] == "-".join([x['SZEV'], x['SZHO'], x['SZNAP']])]

    if (len(database_Subset) == 0):
        continue
    
    for ind, y in database_Subset.iterrows():

        FirstName = y['UNEVEM'].split(" ")
        BirthFirstName = y['SZUNEVE'].split(" ")
        MotherFirstName = y['AUNEVE'].split(" ")

        Employee_Name_Diff = fuzz.token_set_ratio(" ".join([SILC_Name[0], SILC_Name[1]]), " ".join([y['VNEVEM'], FirstName[0]]))
        Employee_BirthName_Diff = fuzz.token_set_ratio(" ".join([SILC_Name[0], SILC_Name[1]]), " ".join([y['SZVNEVE'], BirthFirstName[0]]))
        Mother_Name_Diff = fuzz.token_set_ratio(" ".join([SILC_Mother_Name[0], SILC_Mother_Name[1]]), " ".join([y['AVNEVE'], MotherFirstName[0]]))

        if (Employee_Name_Diff >= 90 and Mother_Name_Diff >= 90):
            with open(file_save, "a") as f:
                f.write(";".join([SILC.loc[index, 'HAZTART'], SILC.loc[index, 'FIXSZ'], SILC.loc[index, 'SZNEV_VIZSGALT'], SILC.loc[index, 'ANYNEV_VIZSGALT'], 
          " ".join([database_Subset.loc[ind, 'VNEVEM'], database_Subset.loc[ind, 'UNEVEM']]), 
          " ".join([database_Subset.loc[ind, 'AVNEVE'], database_Subset.loc[ind, 'AUNEVE']]),
          database_Subset.loc[ind, 'AAJE'],
          "",
          database_Subset.loc[ind, 'SZUL_DAT'],
          SILC.loc[index, 'SZEV'], SILC.loc[index, 'SZHO'], SILC.loc[index, 'SZNAP'], 
          SILC.loc[index, 'FEOR08'],
          str(SILC.loc[index, 'Kategoria'])]))
                f.write("\n")

            SILC.loc[index, 'Talalt'] = 1
            continue

        if (Employee_BirthName_Diff >= 90 and Mother_Name_Diff >= 90):
            with open(file_save, "a") as f:
                f.write(";".join([SILC.loc[index, 'HAZTART'], SILC.loc[index, 'FIXSZ'], SILC.loc[index, 'SZNEV_VIZSGALT'], SILC.loc[index, 'ANYNEV_VIZSGALT'], 
          " ".join([database_Subset.loc[ind, 'SZVNEVE'], database_Subset.loc[ind, 'SZUNEVE']]), 
          " ".join([database_Subset.loc[ind, 'AVNEVE'], database_Subset.loc[ind, 'AUNEVE']]),
          database_Subset.loc[ind, 'AAJE'],
          "",
          database_Subset.loc[ind, 'SZUL_DAT'],
          SILC.loc[index, 'SZEV'], SILC.loc[index, 'SZHO'], SILC.loc[index, 'SZNAP'], 
          SILC.loc[index, 'FEOR08'],
          str(SILC.loc[index, 'Kategoria'])]))
                f.write("\n")

            SILC.loc[index, 'Talalt'] = 1


os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
file_name = 'SILC'
with open(file_name, "wb") as file:
    pickle.dump(SILC, file)