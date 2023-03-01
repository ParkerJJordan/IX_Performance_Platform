import pandas as pd 
import numpy as np


pairname = '41IXA'
dirct = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/data/reference/reference.xlsx'
steps = pd.read_excel(dirct, sheet_name='Steps', index_col='Pair')
steps = steps.loc[steps.index == pairname].values.flatten().tolist()
#steps = steps['Pair'] == pairname
#print(steps)