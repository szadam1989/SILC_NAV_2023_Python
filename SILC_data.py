import pandas as pd
import numpy as np
import os
import pickle

os.chdir("D:/Python/Projects/SILC_NAV_2023/SPSS/")
SILC = pd.read_spss("alkalmazottak_2024.sav")

# SILC.info() 
print(SILC.shape)
print(len(SILC))
print(len(SILC.columns))

SILC['HAZTART'] = SILC['HAZTART'].astype('int64')
SILC['FIXSZ'] = SILC['FIXSZ'].astype('int64')
SILC['SZEV'] = SILC['SZEV'].astype('int64')
SILC['SZHO'] = SILC['SZHO'].astype('int64')
SILC['SZNAP'] = SILC['SZNAP'].fillna(0) # 15 helyen üres értékű a születésnap
SILC['SZNAP'] = SILC['SZNAP'].astype('int64')
SILC['NEME'] = SILC['NEME'].astype('int64')
SILC['FEOR08'] = SILC['FEOR08'].astype('int64')

SILC['HAZTART'] = SILC['HAZTART'].astype('str')
SILC['FIXSZ'] = SILC['FIXSZ'].astype('str')
SILC['SZNEV'] = SILC['SZNEV'].astype('str')
SILC['SZEV'] = SILC['SZEV'].astype('str')
SILC['SZHO'] = SILC['SZHO'].astype('str')
SILC['SZNAP'] = SILC['SZNAP'].astype('str')
SILC['NEME'] = SILC['NEME'].astype('str')
SILC['ANYNEV'] = SILC['ANYNEV'].astype('str')
SILC['FEOR08'] = SILC['FEOR08'].astype('str')

SILC['SZHO'] = SILC['SZHO'].apply('{:0>2}'.format)
SILC['SZNAP'] = SILC['SZNAP'].apply('{:0>2}'.format)
SILC['FEOR08'] = SILC['FEOR08'].apply('{:0>4}'.format)


SILC['SZNEV_VIZSGALT'] = SILC['SZNEV'].str.upper()
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV'].str.upper()

SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('0', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('1', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('2', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('3', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('4', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('5', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('6', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('7', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('8', '')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('9', '')

SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Á', 'A')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ä', 'A')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('É', 'E')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Í', 'I')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ó', 'O')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ö', 'O')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ő', 'O')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ú', 'U')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ü', 'U')
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('Ű', 'U')

SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('DR. ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('DR.', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('IFJ. ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('IFJ.', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('IF. ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('IF.', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('ID. ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('ID.', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('OZV. ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('OZV.', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('.', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('DR ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('IFJ ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('IFJABB ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^IF ', '', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^ID ', '', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('OZV ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(' IFJ', '', regex = False)

SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(' -', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('- ', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace('-', '', regex = False)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'\(', ' ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'\)', ' ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'\s\s\s', ' ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'\s\s', ' ', regex = True)

SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^ +| +$', '', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.lstrip()
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.rstrip()

SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^A ', 'A. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^B ', 'B. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^C ', 'C. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^CS ', 'CS. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^CZ ', 'CZ. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^D ', 'D. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^DZ ', 'DZ. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^DZS ', 'DZS. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^E ', 'E. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^F ', 'F. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^G ', 'G. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^H ', 'H. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^I ', 'I. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^J ', 'J. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^K ', 'K. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^L ', 'L. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^M ', 'M. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^N ', 'N. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^NY ', 'NY. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^O ', 'O. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^P ', 'P. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^Q ', 'Q. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^R ', 'R. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^S ', 'S. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^SZ ', 'SZ. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^T ', 'T. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^TY ', 'TY. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^U ', 'U. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^V ', 'V. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^W ', 'W. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^X ', 'X. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^Y ', 'Y. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^Z ', 'Z. ', regex = True)
SILC['SZNEV_VIZSGALT'] = SILC['SZNEV_VIZSGALT'].str.replace(r'^ZS ', 'ZS. ', regex = True)


SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('0', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('1', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('2', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('3', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('4', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('5', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('6', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('7', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('8', '')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('9', '')

SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Á', 'A')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ä', 'A')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('É', 'E')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Í', 'I')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ó', 'O')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ö', 'O')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ő', 'O')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ú', 'U')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ü', 'U')
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('Ű', 'U')

SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('DR. ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('DR.', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('IFJ. ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('IFJ.', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('IF. ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('IF.', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('ID. ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('ID.', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('OZV. ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('OZV.', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('.', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('DR ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('IFJ ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('IFJABB ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^IF ', '', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^ID ', '', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('OZV ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(' IFJ', '', regex = False)

SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(' -', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('- ', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace('-', '', regex = False)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'\(', ' ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'\)', ' ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'\s\s\s', ' ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'\s\s', ' ', regex = True)

SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^ +| +$', '', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.lstrip()
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.rstrip()

SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^A ', 'A. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^B ', 'B. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^C ', 'C. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^CS ', 'CS. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^CZ ', 'CZ. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^D ', 'D. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^DZ ', 'DZ. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^DZS ', 'DZS. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^E ', 'E. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^F ', 'F. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^G ', 'G. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^H ', 'H. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^I ', 'I. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^J ', 'J. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^K ', 'K. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^L ', 'L. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^M ', 'M. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^N ', 'N. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^NY ', 'NY. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^O ', 'O. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^P ', 'P. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^Q ', 'Q. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^R ', 'R. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^S ', 'S. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^SZ ', 'SZ. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^T ', 'T. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^TY ', 'TY. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^U ', 'U. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^V ', 'V. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^W ', 'W. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^X ', 'X. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^Y ', 'Y. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^Z ', 'Z. ', regex = True)
SILC['ANYNEV_VIZSGALT'] = SILC['ANYNEV_VIZSGALT'].str.replace(r'^ZS ', 'ZS. ', regex = True)

SILC['Kategoria'] = 1

Category_2 = SILC['SZNEV_VIZSGALT'].str.contains('.', regex = False)
SILC['Kategoria'] = np.where(Category_2 == True, 2, SILC['Kategoria'])

Category_3 = SILC['ANYNEV_VIZSGALT'].str.contains('.', regex = False)
SILC['Kategoria'] = np.where(Category_3 == True, 3, SILC['Kategoria'])

Category_4 = SILC['SZNEV_VIZSGALT'].str.contains('.', regex = False) & SILC['ANYNEV_VIZSGALT'].str.contains('.', regex = False)
SILC['Kategoria'] = np.where(Category_4 == True, 4, SILC['Kategoria'])

Category_5 = ~SILC['SZNEV_VIZSGALT'].str.contains(' ')
SILC['Kategoria'] = np.where(Category_5 == True, 5, SILC['Kategoria'])

Category_6 = ~SILC['ANYNEV_VIZSGALT'].str.contains(' ')
SILC['Kategoria'] = np.where(Category_6 == True, 6, SILC['Kategoria'])

Category_7 = ~SILC['SZNEV_VIZSGALT'].str.contains(' ') & ~SILC['ANYNEV_VIZSGALT'].str.contains(' ')
SILC['Kategoria'] = np.where(Category_7 == True, 7, SILC['Kategoria'])

Category_8 = SILC['SZNEV_VIZSGALT'].str.contains('.', regex = False) & ~SILC['ANYNEV_VIZSGALT'].str.contains(' ')
SILC['Kategoria'] = np.where(Category_8 == True, 8, SILC['Kategoria'])

Category_9 = ~SILC['SZNEV_VIZSGALT'].str.contains(' ') & SILC['ANYNEV_VIZSGALT'].str.contains('.', regex = False)
SILC['Kategoria'] = np.where(Category_9 == True, 9, SILC['Kategoria'])

print(len(SILC.loc[SILC['Kategoria'] == 1])) #7335
print(len(SILC.loc[SILC['Kategoria'] == 2])) # 1
print(len(SILC.loc[SILC['Kategoria'] == 3])) # 3
print(len(SILC.loc[SILC['Kategoria'] == 4])) # 1
print(len(SILC.loc[SILC['Kategoria'] == 5])) # 11
print(len(SILC.loc[SILC['Kategoria'] == 6])) # 266
print(len(SILC.loc[SILC['Kategoria'] == 7])) # 12
print(len(SILC.loc[SILC['Kategoria'] == 8])) # 1
print(len(SILC.loc[SILC['Kategoria'] == 9])) # 0

SILC['Talalt'] = 0

print(SILC.loc[SILC['Kategoria'] == 2, "FIXSZ"]) # 6160210002
print(SILC.loc[SILC['Kategoria'] == 2, "SZNEV_VIZSGALT"]) # T. KERTESZ TUNDE
print(SILC.loc[SILC['Kategoria'] == 2, "ANYNEV_VIZSGALT"]) # TKERTESZ JANOSNE
print(SILC.loc[SILC['Kategoria'] == 2, "Kategoria"])

SILC.loc[SILC['Kategoria'] == 2, "SZNEV_VIZSGALT"] = "KERTESZ TUNDE"
SILC.loc[SILC['Kategoria'] == 2, "ANYNEV_VIZSGALT"] = "TKERTESZ JANOSNE"
SILC.loc[SILC['Kategoria'] == 2, "Kategoria"] = 1

print(SILC.loc[SILC['FIXSZ'] == "6160210002"])

os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
file_name = 'SILC'
with open(file_name, "wb") as file:
    pickle.dump(SILC, file)

SILC.info()
# SILC.to_parquet("SILC.parquet", engine = "pyarrow") # Nem működik, pedig nincs is benne datetime