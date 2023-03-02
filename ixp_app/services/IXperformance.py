import pandas as pd
import numpy as np
from qryapsen import AspenConn

class IXPerformance():
    def __init__(self, pairname: str, timespan: int = 365, cyclestart: int = 19):
        self._taglists = TagLists()
        self._aspen = AspenDataPull()

        self.pairname = pairname
        self.timespan = timespan
        self._cyclestart = cyclestart
        self.steps, self.totals, self.analogs = self.tagdata()
        self.resin = self.resinreplacements()
        self.cycles = self.cyclecounter()
        self.performance = self.throughput()

    def cyclecounter(self):
        resin = self.resin.loc[self.resin['UnitName'] == self.pairname]
        #cyccount = self.steps.loc[self.steps['Tag'] == self.pairtag].pivot_table(values='Val', index=['Date'], columns=['Tag'], aggfunc={'Val':np.sum})
        cyccount = self.steps.reset_index().append(resin, ignore_index=True).sort_values(by=['Date']).fillna('Change Out')

        # Total Cycle Count
        totcount = (cyccount[self.pairtag] == self._cyclestart).astype(int)
        cyccount['TotalCycles'] = totcount.groupby(totcount.cummax()).cumsum()

        # Cation Cycle Count
        catcount = (cyccount['ColumnType'] == 'Cation').astype(int)
        cyccount['CationCycles'] = totcount.groupby(catcount.cumsum()).cumsum()

        # Anion Cycle Count
        anicount = (cyccount['ColumnType'] == 'Anion').astype(int)
        cyccount['AnionCycles'] = totcount.groupby(anicount.cumsum()).cumsum()

        # 'Keeper' Count
        cyccount['Keeper'] = totcount.groupby(totcount.cumsum()).cumcount()

        del cyccount['Unit']
        del cyccount['UnitName']
        del cyccount['ColumnType']

        #cyccount = cyccount.reset_index(drop=True)
        return cyccount.reset_index(drop=True)

    def throughput(self): #, steps, totals, analogs, resin, ixdevref, ixstepref):
        taglist, tagdict = self._taglists.full_list(self.pairname)

        # pairtotalizer = ixdevref['LiquorTotal'].loc[ixdevref['Pair'] == pairname][0]
        # acidtotalizer = ixdevref['AcidTotal'].loc[ixdevref['Pair'] == pairname][0]
        # basetotalizer = ixdevref['BaseTotal'].loc[ixdevref['Pair'] == pairname][0]
        # cationrinse = ixdevref['CationRinse'].loc[ixdevref['Pair'] == pairname][0]
        # anionrinse = ixdevref['AnionRinse'].loc[ixdevref['Pair'] == pairname][0]
        # sweetenontotal = ixdevref['SweetenOnTotal'].loc[ixdevref['Pair'] == pairname][0]
        # sweetenofftotal = ixdevref['SweetenOffTotal'].loc[ixdevref['Pair'] == pairname][0]
        # cationbackwash = ixdevref['CationBackwash'].loc[ixdevref['Pair'] == pairname][0]
        # anionbackwash = ixdevref['AnionBackwash'].loc[ixdevref['Pair'] == pairname][0]
        # flow = ixdevref['InletFlow'].loc[ixdevref['Pair'] == pairname][0]
        # sweetenonds = ixdevref['SweetenOnDS'].loc[ixdevref['Pair'] == pairname][0]
        # sweetenoffds = ixdevref['SweetenOffDS'].loc[ixdevref['Pair'] == pairname][0]
        # draincond = ixdevref['DrainConductivity'].loc[ixdevref['Pair'] == pairname][0]
        # recirccond = ixdevref['RecircConductivity'].loc[ixdevref['Pair'] == pairname][0]

        #al = analogs.pivot_table(values='Val', index=['Date'], columns=['Tag'], aggfunc={'Val':np.sum})
        al = self.analogs.loc[:, self.analogs.columns.isin(taglist)]
        al = al.reset_index()

        #tot = totals.pivot_table(values='Val', index=['Date'], columns=['Tag'], aggfunc={'Val':np.sum})
        tot = self.totals.loc[:, self.totals.columns.isin(taglist)]
        tot = tot.reset_index()

        cyc = self.cyclecounter()

        thru = pd.concat([cyc, tot, al], ignore_index=True)
        thru = thru.reset_index().sort_values(by=['Date'])
        thru[[tagdict['StepNumber'], 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']] = thru[[tagdict['StepNumber'], 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']].fillna(method='ffill')
        thru = thru.groupby([tagdict['StepNumber'], 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper'], as_index=False).agg(
            Sequence=(tagdict['SeqNumber'], np.min),
            StepStart=('Date', np.min),
            StepEnd=('Date', np.max),
            Date=('Date', np.mean),
            AvgFlow=(tagdict['InletFlow'], np.mean),
            Conductivity=(tagdict['Conductivity'], np.min),
            SweetenOnDS=(tagdict['SweetenOnDS'], np.max),
            SweetenOffDS=(tagdict['SweetenOffDS'], np.min),
            MinDrainCond=(tagdict['DrainCond'], np.min),
            MinRecircCond=(tagdict['RecircCond'], np.min),
            LiquorTotal=(tagdict['LiquorTotal'], np.max),
            AcidTotal=(tagdict['AcidTotal'], np.max),
            BaseTotal=(tagdict['BaseTotal'], np.max),
            CationRinse=(tagdict['CationRinse'], np.max),
            AnionRinse=(tagdict['AnionRinse'], np.max),
            SweetenOn=(tagdict['SweetenOnTotal'], np.max),
            SweetenOff=(tagdict['SweetenOffTotal'], np.max),
            CationBackwash=(tagdict['CationBackwash'], np.max),
            AnionBackwash=(tagdict['AnionBackwash'], np.max)
            ).sort_values(by=['StepStart']).reset_index()

        # thru['TIS'] = thru['StepStart'] - thru['StepEnd']
        # thru['EOS'] =
        return thru

    def tagdata(self):
        steps_list = self._taglists.steps_list(self.pairname)
        totals_list = self._taglists.totals_list(self.pairname)
        analogs_list = self._taglists.analogs_list(self.pairname)

        steps = self._aspen.search_aspen(taglist=steps_list, query_for='steps', timespan=self.timespan)
        totals = self._aspen.search_aspen(taglist=totals_list, query_for='totals', timespan=self.timespan)
        analogs = self._aspen.search_aspen(taglist=analogs_list, query_for='analogs', timespan=self.timespan)
        return steps, totals, analogs

    def resinreplacements(self):
        resin_replacements_path = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/data/raw/resin.xlsx'
        resin = pd.read_excel(resin_replacements_path, sheet_name=0, parse_dates=['Date'])
        return resin

class TagLists():
    def __init__(self, pairname: str):
        self.pairname = pairname
        self.dirct = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/static/reference/reference.xlsx'

    # List of Aspen step number and sequence tags for the given IX pair
    def steps_list(self):
        steps = pd.read_excel(self.dirct, sheet_name='Steps', index_col='Pair')
        steps = steps.loc[steps.index == self.pairname].values.flatten().tolist()
        return steps

    # List of Aspen analogs tags for the given IX pair
    def analogs_list(self):
        analogs = pd.read_excel(self.dirct, sheet_name='Analogs', index_col='Pair')
        analogs = analogs.loc[analogs.index == self.pairname].values.flatten().tolist()
        return analogs

    # List of Aspen totalizers tags for the given IX pair
    def totals_list(self):
        totals = pd.read_excel(self.dirct, sheet_name='Totals', index_col='Pair')
        totals = totals.loc[totals.index == self.pairname].values.flatten().tolist()
        return totals

    # Full list of all Aspen tags for the given IX pair
    def full_list(self):
        full = pd.read_excel(self.dirct, sheet_name='FullTagList', index_col='Pair')
        full = full.loc[full.index == self.pairname].values.flatten().tolist()
        tagdict = full.loc[full.index == self.pairname].to_dict('list')
        return full, tagdict

class AspenDataPull():
    def __init__(self, taglist: list, query_for: str, timespan: int = 5, server: str ='ARGPCS19'):
        self.taglist = taglist
        self.aspen_server = server
        self.query_for = query_for
        self.timespan = timespan

    # Queries the specified Aspen server for the given tag list
    def search_aspen(self):
        aspen_conn = AspenConn(self.aspen_server, '')

        if self.query_for == 'steps':
            aspen_tag_data = aspen_conn.current(self.taglist, days=self.timespan, request=5)
        elif self.query_for == 'totals':
            aspen_tag_data = aspen_conn.current(self.taglist, days=self.timespan, request=5)
        elif self.query_for == 'analogs':
            aspen_tag_data = aspen_conn.current(self.taglist, days=self.timespan, request=1)
        else:
            aspen_tag_data = None

        return aspen_tag_data