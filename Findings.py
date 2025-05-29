import pickle
import os

file_name = 'SILC'
SILC = None
with open(file_name, "rb") as file:
    SILC = pickle.load(file)

print(len(SILC.loc[SILC['Kategoria'] == 1 & SILC['Talalt'] == 1])) #7341
print(len(SILC.loc[SILC['Kategoria'] == 2])) # 0
print(len(SILC.loc[SILC['Kategoria'] == 3])) # 0
print(len(SILC.loc[SILC['Kategoria'] == 4])) # 0
print(len(SILC.loc[SILC['Kategoria'] == 5])) # 11
print(len(SILC.loc[SILC['Kategoria'] == 6])) # 266
print(len(SILC.loc[SILC['Kategoria'] == 7])) # 12
print(len(SILC.loc[SILC['Kategoria'] == 8])) # 0
print(len(SILC.loc[SILC['Kategoria'] == 9])) # 0