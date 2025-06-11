import pandas as pd
import pickle
import os

os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
file_name = 'SILC'
SILC_FEOR2Jegy = None
with open(file_name, "rb") as file:
    SILC_FEOR2Jegy = pickle.load(file)

SILC_FEOR2Jegy = SILC_FEOR2Jegy.sort_values(by = 'FEOR08')