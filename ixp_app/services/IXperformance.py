import pandas as pd
import numpy as np
from qryapsen import AspenConn

class IXPerformance():
    def __init__(self, pairname, pairtag):
        self.pairname = pairname
        self.pairtag = pairtag
        self._cyclestart = 19

    def cyclecounter(self, steps, resin):
        cnt = steps
        rsn = resin.loc[resin['UnitName'] == self.pairname]
        cnt = cnt.loc[cnt['Tag'] == self.pairtag].pivot_table(values='Val', index=['Date'], columns=['Tag'], aggfunc={'Val':np.sum})
        cnt = cnt.reset_index().append(rsn, ignore_index=True).sort_values(by=['Date']).fillna('Change Out')
        tot = (cnt[self.pairtag] == 19).astype(int)
        cnt['TotalCycles'] = tot.groupby(tot.cummax()).cumsum()
        cat = (cnt['ColumnType'] == 'Cation').astype(int)
        cnt['CationCycles'] = tot.groupby(cat.cumsum()).cumsum()
        ani = (cnt['ColumnType'] == 'Anion').astype(int)
        cnt['AnionCycles'] = tot.groupby(ani.cumsum()).cumsum()
        cnt['Keeper'] = tot.groupby(tot.cumsum()).cumcount()
        del cnt['Unit']
        del cnt['UnitName']
        del cnt['ColumnType']
        cnt = cnt.reset_index(drop=True)
        return cnt

    def throughput(self, steps, totals, analogs, resin, ixdevref, ixstepref):
        devlist = ixdevref.loc[ixdevref['Pair'] == pairname].values.flatten().tolist()
        pairtotalizer = ixdevref['LiquorTotal'].loc[ixdevref['Pair'] == pairname][0]
        acidtotalizer = ixdevref['AcidTotal'].loc[ixdevref['Pair'] == pairname][0]
        basetotalizer = ixdevref['BaseTotal'].loc[ixdevref['Pair'] == pairname][0]
        cationrinse = ixdevref['CationRinse'].loc[ixdevref['Pair'] == pairname][0]
        anionrinse = ixdevref['AnionRinse'].loc[ixdevref['Pair'] == pairname][0]
        sweetenontotal = ixdevref['SweetenOnTotal'].loc[ixdevref['Pair'] == pairname][0]
        sweetenofftotal = ixdevref['SweetenOffTotal'].loc[ixdevref['Pair'] == pairname][0]
        cationbackwash = ixdevref['CationBackwash'].loc[ixdevref['Pair'] == pairname][0]
        anionbackwash = ixdevref['AnionBackwash'].loc[ixdevref['Pair'] == pairname][0]
        flow = ixdevref['InletFlow'].loc[ixdevref['Pair'] == pairname][0]
        sweetenonds = ixdevref['SweetenOnDS'].loc[ixdevref['Pair'] == pairname][0]
        sweetenoffds = ixdevref['SweetenOffDS'].loc[ixdevref['Pair'] == pairname][0]
        draincond = ixdevref['DrainConductivity'].loc[ixdevref['Pair'] == pairname][0]
        recirccond = ixdevref['RecircConductivity'].loc[ixdevref['Pair'] == pairname][0]

        al = analogs.pivot_table(values='Val', index=['Date'], columns=['Tag'], aggfunc={'Val':np.sum})
        al = al.loc[:, al.columns.isin(devlist)]
        al = al.reset_index()
        
        tot = totals.pivot_table(values='Val', index=['Date'], columns=['Tag'], aggfunc={'Val':np.sum})
        tot = tot.loc[:, tot.columns.isin(devlist)]
        tot = tot.reset_index()
        
        cyc = CationAnionCycleCounter(pairname, pairtag, steps, resin)
        
        thru = pd.concat([cyc, tot, al], ignore_index=True)
        thru = thru.reset_index().sort_values(by=['Date'])
        thru[[pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']] = thru[[pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']].fillna(method='ffill')
        thru = thru.groupby([pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper'], as_index=False).agg(
            StepStart=('Date', np.min),
            StepEnd=('Date', np.max),
            Date=('Date', np.mean),
            LiquorTotal=(pairtotalizer, np.max),
            AcidTotal=(acidtotalizer, np.max),
            BaseTotal=(basetotalizer, np.max),
            CationRinse=(cationrinse, np.max),
            AnionRinse=(anionrinse, np.max),
            SweetenOn=(sweetenontotal, np.max),
            SweetenOff=(sweetenofftotal, np.max),
            CationBackwash=(cationbackwash, np.max),
            AnionBackwash=(anionbackwash, np.max),
            AvgFlow=(flow, np.max),
            SweetenOnDS=(sweetenonds, np.max),
            SweetenOffDS=(sweetenoffds, np.max),
            MinDrainCond=(draincond, np.max),
            MinRecircCond=(recirccond, np.max)
            ).sort_values(by=['StepStart']).reset_index()
        return thru

class TagLists():
    def __init__(self, pairname):
        self.pairname = pairname
        self.dirct = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/data/reference/reference.xlsx'

    def steps_list(self):
        steps = pd.read_excel(self.dirct, sheet_name='Steps', index_col='Pair')
        steps = steps.loc[steps.index == self.pairname].values.flatten().tolist()
        return steps
    
    def analogs_list(self):
        analogs = pd.read_excel(self.dirct, sheet_name='Analogs', index_col='Pair')
        analogs = analogs.loc[analogs.index == self.pairname].values.flatten().tolist()
        return analogs
    
    def totals_list(self):
        totals = pd.read_excel(self.dirct, sheet_name='Totals', index_col='Pair')
        totals = totals.loc[totals.index == self.pairname].values.flatten().tolist()
        return totals
    
class AspenDataPull():
    def __init__(self, taglist: list, query_for: str, timespan: int = 5, server: str ='ARGPCS19'):
        self.taglist = taglist
        self.aspen_server = server
        self.query_for = query_for
        self.timespan = timespan

    def search_aspen(self):
        aspen_conn = AspenConn(self.aspen_server, '')

        if self.query_for == 'steps':
            aspen_tag_data = aspen_conn.current(self.taglist, days=self.timespan, request=5, pivot=True)

        elif self.query_for == 'totals':
            aspen_tag_data = aspen_conn.current(self.taglist, days=self.timespan, request=5, pivot=True)

        elif self.query_for == 'analogs':
            aspen_tag_data = aspen_conn.current(self.taglist, days=self.timespan, request=1, pivot=True)

        else:
            aspen_tag_data = None

        return aspen_tag_data
    
tag_list = ['F41311_PV', 'C41322_PV', 'DE41374_PV', 'DE41373_PV', 'C41273_PV', 'C41270_PV']
aspen = AspenDataPull(taglist=tag_list, query_for='analogs', timespan=10)
test = aspen.search_aspen()
print(test)