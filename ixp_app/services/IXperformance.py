import pandas as pd
import numpy as np
from ixp_app.services.qryapsen import AspenConn
#from qryapsen import AspenConn

class IXPerformance():
    def __init__(self, pairname: str, timespan= 5):
        self._taglists = TagLists(pairname)
        self._aspen = AspenDataPull(pairname)
        self._cyclestart = 19
        self._pairtag = self._taglists.full_list()[1]['StepNumber'][0]

        self.pairname = pairname
        self.timespan = timespan

        self.steps, self.totals, self.analogs = self.tagdata()
        self.resin = self.resinreplacements()
        self.cycles = self.cyclecounter()
        self.performance = self.throughput()

    def cyclecounter(self):
        resin = self.resin.loc[self.resin['UnitName'] == self.pairname]

        cyccount = pd.concat([self.steps, resin])
        cyccount['TS'] = pd.to_datetime(cyccount['TS'])
        cyccount = cyccount.sort_values(by=['TS']).reset_index(drop=True)#.fillna({'VALUE': 'Change Out'})

        # Total Cycle Count
        totcount = (cyccount['VALUE'] == self._cyclestart).astype(int)
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

        return cyccount.rename(columns={"VALUE": self._pairtag}).reset_index(drop=True)

    def throughput(self): #, steps, totals, analogs, resin, ixdevref, ixstepref):
        taglist, tagdict = self._taglists.full_list()

        al = self.analogs.loc[:, self.analogs.columns.isin(taglist)]
        al = al.reset_index()

        tot = self.totals.loc[:, self.totals.columns.isin(taglist)]
        tot = tot.reset_index()

        cyc = self.cyclecounter()

        thru = pd.concat([cyc, tot, al], ignore_index=True)
        thru['TS'] = pd.to_datetime(thru['TS'])
        thru = thru.sort_values(by=['TS']).reset_index(drop=True)
        thru[[self._pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']] = thru[[self._pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']].fillna(method='ffill')

        thru = thru.groupby([self._pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper'], as_index=False).agg(
            StepStart=('TS', np.min),
            StepEnd=('TS', np.max),
            Date=('TS', np.mean),
            AvgFlow=(tagdict['InletFlow'][0], np.mean),
            Conductivity=(tagdict['Conductivity'][0], np.min),
            SweetenOnDS=(tagdict['SweetenOnDS'][0], np.max),
            SweetenOffDS=(tagdict['SweetenOffDS'][0], np.min),
            MinDrainCond=(tagdict['DrainCond'][0], np.min),
            MinRecircCond=(tagdict['RecircCond'][0], np.min),
            LiquorTotal=(tagdict['LiquorTotal'][0], np.max),
            AcidTotal=(tagdict['AcidTotal'][0], np.max),
            BaseTotal=(tagdict['BaseTotal'][0], np.max),
            CationRinse=(tagdict['CationRinse'][0], np.max),
            AnionRinse=(tagdict['AnionRinse'][0], np.max),
            SweetenOn=(tagdict['SweetenOnTotal'][0], np.max),
            SweetenOff=(tagdict['SweetenOffTotal'][0], np.max),
            CationBackwash=(tagdict['CationBackwash'][0], np.max),
            AnionBackwash=(tagdict['AnionBackwash'][0], np.max)
            ).sort_values(by=['StepStart']).reset_index(drop=True)
        thru.to_clipboard()
        # thru['TIS'] = thru['StepStart'] - thru['StepEnd']
        # thru['EOS'] =
        return thru

    def tagdata(self):
        steps_list = self._taglists.steps_list()
        totals_list = self._taglists.totals_list()
        analogs_list = self._taglists.analogs_list()

        steps = self._aspen.search_aspen(taglist=steps_list, query_for='steps', timespan=self.timespan)
        steps.columns = steps.columns.droplevel(-1)
        totals = self._aspen.search_aspen(taglist=totals_list, query_for='totals', timespan=self.timespan)
        analogs = self._aspen.search_aspen(taglist=analogs_list, query_for='analogs', timespan=self.timespan)
        return steps, totals, analogs

    def resinreplacements(self):
        resin_replacements_path = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/data/raw/resin.xlsx'
        resin = pd.read_excel(resin_replacements_path, sheet_name=0, parse_dates=['TS'])
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
        fulllist = full.loc[full.index == self.pairname].values.flatten().tolist()
        tagdict = full.loc[full.index == self.pairname].to_dict('list')
        return fulllist, tagdict

class AspenDataPull():
    def __init__(self, server: str ='ARGPCS19'):
        self.aspen_server = 'ARGPCS19'

    # Queries the specified Aspen server for the given tag list
    def search_aspen(self, taglist: list, query_for: str, timespan=5):
        aspen_conn = AspenConn(self.aspen_server, '')

        if query_for == 'steps' and len(taglist) == 1:
            aspen_tag_data = aspen_conn.current(taglist[0], days=timespan, request=5, single_pivot=True)
        elif query_for == 'totals':
            aspen_tag_data = aspen_conn.current(taglist, days=timespan, request=5)
        elif query_for == 'analogs':
            aspen_tag_data = aspen_conn.current(taglist, days=timespan, request=1)
        else:
            aspen_tag_data = None

        return aspen_tag_data
    
test = IXPerformance(pairname='41IXA').performance
test.to_clipboard()