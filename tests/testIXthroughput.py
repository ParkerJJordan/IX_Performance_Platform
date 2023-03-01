import pandas as pd
import numpy as np
from ixp_app.services.qryapsen import AspenConn
aspen_server = 'ARGPCS19'
aspen_conn = AspenConn(aspen_server, '')

# dirc = 'C:/Users/pjordan/ProgramingProjects/IX_Cycle_Counter/cycledata.xlsx'
# stepnumbers = pd.read_excel(dirc, sheet_name=0, parse_dates = ['Date'])
# totalizers = pd.read_excel(dirc, sheet_name=1, parse_dates = ['Date'])
# analogdata = pd.read_excel(dirc, sheet_name=2, parse_dates = ['Date'])
# resindata = pd.read_excel(dirc, sheet_name=3, parse_dates = ['Date'])
# ixdevref = pd.read_excel(dirc, sheet_name=4)
# ixstepref = pd.read_excel(dirc, sheet_name=5)

# tag_list = ['REF1_UNIT16_SN', 'REF1_UNIT17_SN', 'REF1_UNIT18_SN']
tag_list = ['F41311_PV', 'C41322_PV', 'DE41374_PV', 'DE41373_PV', 'C41273_PV', 'C41270_PV']
pairtag = 'REF1_UNIT16_SN'
pairname = '41IXA'

result = aspen_conn.current(tag_list, days=5, request=1)
print(result)
