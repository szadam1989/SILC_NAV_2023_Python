import pandas as pd
import oracledb
import getpass
import pickle
import os

pd.options.mode.chained_assignment = None

username = getpass.getuser()
password = getpass.getpass(f"Kérlek, add meg a(z) {username} felhasználói nevedhez tartozó jelszót: ")

database = oracledb.makedsn(host = "emerald.ksh.hu", port = "1521", service_name = "emerald.ksh.hu")
conn = oracledb.connect(user = username, password = password, dsn = database)
cur = conn.cursor()

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where (upper(VNEVEM) like 'A%' or upper(VNEVEM) like 'Á%') 
order by upper(VNEVEM), upper(UNEVEM)"""
database_A = pd.read_sql(query, con = conn, dtype = str)
print(database_A.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'B%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_B = pd.read_sql(query, con = conn, dtype = str)
print(database_B.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'C%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_C = pd.read_sql(query, con = conn, dtype = str)
print(database_C.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'D%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_D = pd.read_sql(query, con = conn, dtype = str)
print(database_D.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where (upper(VNEVEM) like 'E%' or upper(VNEVEM) like 'É%') 
order by upper(VNEVEM), upper(UNEVEM)"""
database_E = pd.read_sql(query, con = conn, dtype = str)
print(database_E.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'F%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_F = pd.read_sql(query, con = conn, dtype = str)
print(database_F.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'G%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_G = pd.read_sql(query, con = conn, dtype = str)
print(database_G.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'H%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_H = pd.read_sql(query, con = conn, dtype = str)
print(database_H.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where (upper(VNEVEM) like 'I%' or upper(VNEVEM) like 'Í%') 
order by upper(VNEVEM), upper(UNEVEM)"""
database_I = pd.read_sql(query, con = conn, dtype = str)
print(database_I.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'J%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_J = pd.read_sql(query, con = conn, dtype = str)
print(database_J.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'K%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_K = pd.read_sql(query, con = conn, dtype = str)
print(database_K.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'L%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_L = pd.read_sql(query, con = conn, dtype = str)
print(database_L.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'M%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_M = pd.read_sql(query, con = conn, dtype = str)
print(database_M.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'N%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_N = pd.read_sql(query, con = conn, dtype = str)
print(database_N.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 
where (upper(VNEVEM) like 'O%' or upper(VNEVEM) like 'Ó%' or upper(VNEVEM) like 'Ö%' or upper(VNEVEM) like 'Ő%') 
order by upper(VNEVEM), upper(UNEVEM)"""
database_O = pd.read_sql(query, con = conn, dtype = str)
print(database_O.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'P%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_P = pd.read_sql(query, con = conn, dtype = str)
print(database_P.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'Q%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_Q = pd.read_sql(query, con = conn, dtype = str)
print(database_Q.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'R%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_R = pd.read_sql(query, con = conn, dtype = str)
print(database_R.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'S%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_S = pd.read_sql(query, con = conn, dtype = str)
print(database_S.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'T%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_T = pd.read_sql(query, con = conn, dtype = str)
print(database_T.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 
where (upper(VNEVEM) like 'U%' or upper(VNEVEM) like 'Ú%' or upper(VNEVEM) like 'Ü%' or upper(VNEVEM) like 'Ű%') 
order by upper(VNEVEM), upper(UNEVEM)"""
database_U = pd.read_sql(query, con = conn, dtype = str)
print(database_U.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'V%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_V = pd.read_sql(query, con = conn, dtype = str)
print(database_V.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'W%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_W = pd.read_sql(query, con = conn, dtype = str)
print(database_W.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'X%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_X = pd.read_sql(query, con = conn, dtype = str)
print(database_X.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'Y%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_Y = pd.read_sql(query, con = conn, dtype = str)
print(database_Y.shape)

query = """select distinct AAJE, upper(VNEVEM) VNEVEM, upper(UNEVEM) UNEVEM, upper(SZVNEVE) SZVNEVE, 
upper(SZUNEVE) SZUNEVE, upper(AVENVE) AVNEVE, upper(AUNEVE) AUNEVE, 
to_char(lg.lg_naptar_uj.szulido_adoazbol(AAJE), 'YYYY-MM-DD') SZUL_DAT 
from LG23.VNAA0_2326_230112_V00 where upper(VNEVEM) like 'Z%' 
order by upper(VNEVEM), upper(UNEVEM)"""
database_Z = pd.read_sql(query, con = conn, dtype = str)
print(database_Z.shape)

cur.close()

conn.close()

database = pd.concat([database_A, database_B, database_C, database_D, database_E, 
           database_F, database_G, database_H, database_I, database_J, 
          database_K, database_L, database_M, database_N, database_O, 
          database_P, database_Q, database_R, database_S, database_T, 
          database_U, database_V, database_W, database_X, database_Y, 
          database_Z, ], ignore_index = True)

print(database.shape) # 4.921.576 sor és 8 oszlop

database['VNEVEM'] = database['VNEVEM'].str.replace('0', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('1', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('2', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('3', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('4', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('5', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('6', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('7', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('8', '')
database['VNEVEM'] = database['VNEVEM'].str.replace('9', '')

database['VNEVEM'] = database['VNEVEM'].str.replace('Á', 'A')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ä', 'A')
database['VNEVEM'] = database['VNEVEM'].str.replace('É', 'E')
database['VNEVEM'] = database['VNEVEM'].str.replace('Í', 'I')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ó', 'O')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ö', 'O')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ő', 'O')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ú', 'U')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ü', 'U')
database['VNEVEM'] = database['VNEVEM'].str.replace('Ű', 'U')

database['VNEVEM'] = database['VNEVEM'].str.replace('DR.', '', regex = False)
database['VNEVEM'] = database['VNEVEM'].str.replace('IFJ.', '', regex = False)

database['VNEVEM'] = database['VNEVEM'].str.replace('.', '', regex = False)
database['VNEVEM'] = database['VNEVEM'].str.replace('-', '')

database['VNEVEM'] = database['VNEVEM'].str.replace('DR ', '', regex = False)
database['VNEVEM'] = database['VNEVEM'].str.replace('IFJ ', '', regex = False)

database['VNEVEM'] = database['VNEVEM'].str.replace(r'\s\s', ' ', regex = True)
database['VNEVEM'] = database['VNEVEM'].str.replace(r'^ +| +$', '', regex = True)
database['VNEVEM'] = database['VNEVEM'].str.lstrip()
database['VNEVEM'] = database['VNEVEM'].str.rstrip()



database['UNEVEM'] = database['UNEVEM'].str.replace('0', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('1', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('2', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('3', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('4', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('5', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('6', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('7', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('8', '')
database['UNEVEM'] = database['UNEVEM'].str.replace('9', '')

database['UNEVEM'] = database['UNEVEM'].str.replace('Á', 'A')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ä', 'A')
database['UNEVEM'] = database['UNEVEM'].str.replace('É', 'E')
database['UNEVEM'] = database['UNEVEM'].str.replace('Í', 'I')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ó', 'O')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ö', 'O')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ő', 'O')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ú', 'U')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ü', 'U')
database['UNEVEM'] = database['UNEVEM'].str.replace('Ű', 'U')

database['UNEVEM'] = database['UNEVEM'].str.replace('DR.', '', regex = False)
database['UNEVEM'] = database['UNEVEM'].str.replace('IFJ.', '', regex = False)

database['UNEVEM'] = database['UNEVEM'].str.replace('.', '', regex = False)
database['UNEVEM'] = database['UNEVEM'].str.replace('-', '')

database['UNEVEM'] = database['UNEVEM'].str.replace('DR ', '', regex = False)
database['UNEVEM'] = database['UNEVEM'].str.replace('IFJ ', '', regex = False)

database['UNEVEM'] = database['UNEVEM'].str.replace(r'\s\s', ' ', regex = True)
database['UNEVEM'] = database['UNEVEM'].str.replace(r'^ +| +$', '', regex = True)
database['UNEVEM'] = database['UNEVEM'].str.lstrip()
database['UNEVEM'] = database['UNEVEM'].str.rstrip()


database['SZVNEVE'] = database['SZVNEVE'].str.replace('0', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('1', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('2', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('3', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('4', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('5', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('6', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('7', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('8', '')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('9', '')

database['SZVNEVE'] = database['SZVNEVE'].str.replace('Á', 'A')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ä', 'A')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('É', 'E')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Í', 'I')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ó', 'O')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ö', 'O')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ő', 'O')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ú', 'U')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ü', 'U')
database['SZVNEVE'] = database['SZVNEVE'].str.replace('Ű', 'U')

database['SZVNEVE'] = database['SZVNEVE'].str.replace('DR.', '', regex = False)
database['SZVNEVE'] = database['SZVNEVE'].str.replace('IFJ.', '', regex = False)

database['SZVNEVE'] = database['SZVNEVE'].str.replace('.', '', regex = False)
database['SZVNEVE'] = database['SZVNEVE'].str.replace('-', '')

database['SZVNEVE'] = database['SZVNEVE'].str.replace('DR ', '', regex = False)
database['SZVNEVE'] = database['SZVNEVE'].str.replace('IFJ ', '', regex = False)

database['SZVNEVE'] = database['SZVNEVE'].str.replace(r'\s\s', ' ', regex = True)
database['SZVNEVE'] = database['SZVNEVE'].str.replace(r'^ +| +$', '', regex = True)
database['SZVNEVE'] = database['SZVNEVE'].str.lstrip()
database['SZVNEVE'] = database['SZVNEVE'].str.rstrip()


database['SZUNEVE'] = database['SZUNEVE'].str.replace('0', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('1', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('2', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('3', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('4', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('5', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('6', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('7', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('8', '')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('9', '')

database['SZUNEVE'] = database['SZUNEVE'].str.replace('Á', 'A')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ä', 'A')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('É', 'E')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Í', 'I')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ó', 'O')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ö', 'O')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ő', 'O')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ú', 'U')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ü', 'U')
database['SZUNEVE'] = database['SZUNEVE'].str.replace('Ű', 'U')

database['SZUNEVE'] = database['SZUNEVE'].str.replace('DR.', '', regex = False)
database['SZUNEVE'] = database['SZUNEVE'].str.replace('IFJ.', '', regex = False)

database['SZUNEVE'] = database['SZUNEVE'].str.replace('.', '', regex = False)
database['SZUNEVE'] = database['SZUNEVE'].str.replace('-', '')

database['SZUNEVE'] = database['SZUNEVE'].str.replace('DR ', '', regex = False)
database['SZUNEVE'] = database['SZUNEVE'].str.replace('IFJ ', '', regex = False)

database['SZUNEVE'] = database['SZUNEVE'].str.replace(r'\s\s', ' ', regex = True)
database['SZUNEVE'] = database['SZUNEVE'].str.replace(r'^ +| +$', '', regex = True)
database['SZUNEVE'] = database['SZUNEVE'].str.lstrip()
database['SZUNEVE'] = database['SZUNEVE'].str.rstrip()


database['AVNEVE'] = database['AVNEVE'].str.replace('0', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('1', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('2', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('3', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('4', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('5', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('6', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('7', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('8', '')
database['AVNEVE'] = database['AVNEVE'].str.replace('9', '')

database['AVNEVE'] = database['AVNEVE'].str.replace('Á', 'A')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ä', 'A')
database['AVNEVE'] = database['AVNEVE'].str.replace('É', 'E')
database['AVNEVE'] = database['AVNEVE'].str.replace('Í', 'I')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ó', 'O')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ö', 'O')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ő', 'O')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ú', 'U')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ü', 'U')
database['AVNEVE'] = database['AVNEVE'].str.replace('Ű', 'U')

database['AVNEVE'] = database['AVNEVE'].str.replace('DR.', '', regex = False)
database['AVNEVE'] = database['AVNEVE'].str.replace('IFJ.', '', regex = False)

database['AVNEVE'] = database['AVNEVE'].str.replace('.', '', regex = False)
database['AVNEVE'] = database['AVNEVE'].str.replace('-', '')

database['AVNEVE'] = database['AVNEVE'].str.replace('DR ', '', regex = False)
database['AVNEVE'] = database['AVNEVE'].str.replace('IFJ ', '', regex = False)

database['AVNEVE'] = database['AVNEVE'].str.replace(r'\s\s', ' ', regex = True)
database['AVNEVE'] = database['AVNEVE'].str.replace(r'^ +| +$', '', regex = True)
database['AVNEVE'] = database['AVNEVE'].str.lstrip()
database['AVNEVE'] = database['AVNEVE'].str.rstrip()


database['AUNEVE'] = database['AUNEVE'].str.replace('0', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('1', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('2', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('3', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('4', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('5', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('6', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('7', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('8', '')
database['AUNEVE'] = database['AUNEVE'].str.replace('9', '')

database['AUNEVE'] = database['AUNEVE'].str.replace('Á', 'A')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ä', 'A')
database['AUNEVE'] = database['AUNEVE'].str.replace('É', 'E')
database['AUNEVE'] = database['AUNEVE'].str.replace('Í', 'I')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ó', 'O')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ö', 'O')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ő', 'O')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ú', 'U')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ü', 'U')
database['AUNEVE'] = database['AUNEVE'].str.replace('Ű', 'U')

database['AUNEVE'] = database['AUNEVE'].str.replace('DR.', '', regex = False)
database['AUNEVE'] = database['AUNEVE'].str.replace('IFJ.', '', regex = False)

database['AUNEVE'] = database['AUNEVE'].str.replace('.', '', regex = False)
database['AUNEVE'] = database['AUNEVE'].str.replace('-', '')

database['AUNEVE'] = database['AUNEVE'].str.replace('DR ', '', regex = False)
database['AUNEVE'] = database['AUNEVE'].str.replace('IFJ ', '', regex = False)

database['AUNEVE'] = database['AUNEVE'].str.replace(r'\s\s', ' ', regex = True)
database['AUNEVE'] = database['AUNEVE'].str.replace(r'^ +| +$', '', regex = True)
database['AUNEVE'] = database['AUNEVE'].str.lstrip()
database['AUNEVE'] = database['AUNEVE'].str.rstrip()

file_name = 'database.parquet'
os.chdir("D:/Python/Projects/SILC_NAV_2023/Environment/")
database.to_parquet(file_name, engine = "pyarrow" )