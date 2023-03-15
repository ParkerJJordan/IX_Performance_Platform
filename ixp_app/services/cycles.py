import pandas as pd
import numpy as np
import datetime as dt
from ixp_app.services.qryapsen import AspenConn
from azure.storage.blob import BlobClient
#from qryapsen import AspenConn

class CyclePerformance():
    def __init__(self, pairname:str, timespan=5, starting_step:int=19, cycle_offset:int=5):
        self.pairname = pairname
        self.timespan = timespan
        self._cyclestart = starting_step
        self._cycle_offset = cycle_offset
        self.start_date = '2023-03-01 00:00:00'
        self.resin, self._start_date, self._cation_vol, self._anion_vol = self.resinreplacements()
        self.steps, self.totals, self.analogs, self.pairtag, self.step_names, self.step_types= self.tagdata()

    def cyclecounter(self):
        '''
        Building the Cycle Counts table for an Ion Exchange pair

        ----------------Cycle Counts----------------
        Date
        Step Number
        Total Cycles
        Cation Cycles
        Anion Cycles
        Keeper
        ----------------------------------------------
        '''
        cyccount = pd.concat([self.steps, self.resin], ignore_index=True)
        cyccount['TS'] = pd.to_datetime(cyccount['TS'])
        cyccount = cyccount.sort_values(by=['TS']).reset_index(drop=True).fillna({self.pairtag: 0})

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
        del cyccount['Volume']

        return cyccount.reset_index(drop=True).apply(pd.to_numeric)

    def throughput(self):
        '''
        Building the Throughput table for an Ion Exchange pair

        ----------------Cycle Counts----------------
        Step Number
        Total Cycles
        Cation Cycles
        Anion Cycles
        Keeper
        Step Start
        Step End
        Date
        Average Flow
        Minimum Conductivity
        Sweeten-On DS
        Sweeten-Off DS
        Minimum Drain Conductivity
        Minimum Recirc Conductivity
        Column Throughput
        Acid Total
        Base Total
        Cation Rinse
        Anion Rinse
        Sweeten-On Throughput
        Sweeten-off Throughput
        Cation Backwash
        Anion Backwash 

        ----------------------------------------------
        '''
        taglist, tagdict = TagLists(self.pairname).full_list()
        taglist.append('TS')

        al = self.analogs.loc[:, self.analogs.columns.isin(taglist)]
        tot = self.totals.loc[:, self.totals.columns.isin(taglist)]
        cyc = self.cyclecounter()

        thru = pd.concat([cyc, tot, al], ignore_index=True)
        thru['TS'] = pd.to_datetime(thru['TS'])
        thru = thru.sort_values(by=['TS']).reset_index(drop=True)
        thru[[self.pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']] = thru[[self.pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper']].fillna(method='ffill')
        thru = thru.groupby([self.pairtag, 'TotalCycles', 'CationCycles', 'AnionCycles', 'Keeper'], as_index=False).agg(
            StepStart=('TS', np.min),
            StepEnd=('TS', np.max),
            Date=('TS', np.mean),
            AvgFlow=(tagdict.get('InletFlow').get(self.pairname), np.mean),
            Conductivity=(tagdict.get('Conductivity').get(self.pairname), np.max),
            SweetenOnDS=(tagdict.get('SweetenOnDS').get(self.pairname), np.max),
            SweetenOffDS=(tagdict.get('SweetenOffDS').get(self.pairname), np.min),
            MinDrainCond=(tagdict.get('DrainCond').get(self.pairname), np.min),
            MinRecircCond=(tagdict.get('RecircCond').get(self.pairname), np.min),
            LiquorTotal=(tagdict.get('LiquorTotal').get(self.pairname), np.max),
            AcidTotal=(tagdict.get('AcidTotal').get(self.pairname), np.max),
            BaseTotal=(tagdict.get('BaseTotal').get(self.pairname), np.max),
            CationSlowRinse=(tagdict.get('CationSlowRinse').get(self.pairname), np.max),
            AnionSlowRinse=(tagdict.get('AnionSlowRinse').get(self.pairname), np.max),
            CationFastRinse=(tagdict.get('CationFastRinse').get(self.pairname), np.max),
            AnionFastRinse=(tagdict.get('AnionFastRinse').get(self.pairname), np.max),
            SweetenOn=(tagdict.get('SweetenOnTotal').get(self.pairname), np.max),
            SweetenOff=(tagdict.get('SweetenOffTotal').get(self.pairname), np.max),
            CationBackwash=(tagdict.get('CationBackwash').get(self.pairname), np.max),
            AnionBackwash=(tagdict.get('AnionBackwash').get(self.pairname), np.max)
            ).sort_values(by=['StepStart']).reset_index(drop=True)
        thru['TIS [min]'] = ((thru['StepEnd'] - thru['StepStart']).dt.seconds)/60
        thru = thru.merge(self.step_names, how='left', left_on=self.pairtag, right_on='StepNumber')
        thru = thru.merge(self.step_types, how='left', left_on=self.pairtag, right_on='StepNumber')
        return thru
    
    def kpis(self):
        '''
        Building the KPI table for an Ion Exchange pair

        ----------------KPIs Per Cycle----------------
        Total Throughput                            [mtds]
        Final Primary Service Breakthrough Point    [pH or mS/cm]
        Syrup Throughput per Resin Volume           [mtds/ft3 anion resin]
        Unitary Chemical Usage                      [kgds/mtds]
        Total Water Usage                           [m3/mtds]
        Sweetwater Generation                       [m3/mtds]
            Sweeten Off Final Concentration Anion   [%DS]
        Waste Water Generation                      [m3/mtds]
            Anion Rinse                             [m3/mtds]
        ----------------------------------------------
        '''
        kpi = pd.DataFrame(columns=['Cycle',
                                    'Cycle End Time',
                                    'Total Throughput', 
                                    'Final Primary Service Breakthrough Point', 
                                    'Syrup Throughput per Resin Volume',
                                    'Acid Chemical Usage',
                                    'Base Chemical Usage',
                                    'Total Water Usage', 
                                    'Sweetwater Generation',
                                    'Waste Water Generation'
                                    ])
        throughtput = self.throughput()
        throughtput.to_clipboard()
        prev_cycles = range(int(max(throughtput['TotalCycles'])-1), int(max(throughtput['TotalCycles'])-1)-self._cycle_offset, -1)
        for cyc in prev_cycles:
            thru = throughtput.loc[throughtput['TotalCycles'] == cyc]
            cyc_end = thru['StepEnd'].max(skipna=True)
            total_thru = thru['LiquorTotal'].loc[thru['StepType'] == 'Primary Service'].max(skipna=True)*0.133681*70.6*0.34/2205
            final_cond = thru['Conductivity'].loc[thru['StepType'] == 'Primary Service'].max(skipna=True)
            acid_usage = thru['AcidTotal'].loc[thru['StepType'] == 'Chemical Regen'].max(skipna=True)
            base_usage = thru['BaseTotal'].loc[thru['StepType'] == 'Chemical Regen'].max(skipna=True)

            backwash_water = (thru['AnionBackwash'].loc[thru['StepType'] == 'Backwash'].max(skipna=True) +
                                thru['CationBackwash'].loc[thru['StepType'] == 'Backwash'].max(skipna=True))

            
            rinse_water = (thru['AnionSlowRinse'].loc[thru['StepType'] == 'Slow Rinse'].max(skipna=True) + 
                                thru['CationSlowRinse'].loc[thru['StepType'] == 'Slow Rinse'].max(skipna=True) + 
                                thru['AnionFastRinse'].loc[thru['StepType'] == 'Fast Rinse'].max(skipna=True) + 
                                thru['CationFastRinse'].loc[thru['StepType'] == 'Fast Rinse'].max(skipna=True))

            # sweetenoff_water = thru[thru['StepType'].astype(str).str.contains('Sweeten Off')]
            sweetenoff_water = (thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 1'].max(skipna=True) +
                                thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 2'].max(skipna=True) + 
                                thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 3'].max(skipna=True) +
                                thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 4'].max(skipna=True) +
                                thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 6'].max(skipna=True))
            
            chem_dilut  = ((acid_usage/264.172)*(72.34 - 72.34*(0.07/0.36)/1000) + (base_usage/264.172)*(39.6 - 39.6*(0.045/0.32)/1000))
            water_usage = sweetenoff_water + backwash_water + rinse_water + chem_dilut

            sweetwater_gen = (thru['SweetenOn'].loc[thru['StepType'] == 'Sweeten On 2'].max(skipna=True) + 
                              thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 3'].max(skipna=True) +
                              thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 4'].max(skipna=True))
            
            waste_gen = (acid_usage + 
                         base_usage +
                         backwash_water +
                         rinse_water + 
                         pd.concat([pd.Series([0]),(thru['SweetenOn'].loc[thru['StepType'] == 'Sweeten On 1'])]).max(skipna=True) +
                         pd.concat([pd.Series([0]), (thru['SweetenOff'].loc[thru['StepType'] == 'Sweeten Off 5'].replace(np.nan,0))]).max(skipna=True))
            
            thru_per_resin = total_thru/self._anion_vol

            kpi_list = [cyc, cyc_end, total_thru, final_cond, thru_per_resin, 
                        (acid_usage*72.34/0.264172/1000)/total_thru, (base_usage*39.6/0.264172/1000)/total_thru, water_usage/total_thru, 
                        sweetwater_gen/total_thru, waste_gen/total_thru]
            kpi.loc[len(kpi)] = kpi_list

        return kpi

    def tagdata(self):
        _taglists = TagLists(self.pairname)
        _aspen = AspenDataPull()
        _alltags, _tagdict = _taglists.full_list()

        _pairtag = _tagdict.get('StepNumber').get(self.pairname)
        steps_list = _taglists.steps_list()
        totals_list = _taglists.totals_list()
        analogs_list = _taglists.analogs_list()
        step_names = _taglists.step_names()
        step_types = _taglists.step_types()

        steps = _aspen.search_aspen(taglist=steps_list, query_for='steps', timespan=self.timespan, date_limit=self.start_date)
        totals = _aspen.search_aspen(taglist=totals_list, query_for='totals', timespan=self.timespan, date_limit=self.start_date)
        analogs = _aspen.search_aspen(taglist=analogs_list, query_for='analogs', timespan=self.timespan, date_limit=self.start_date)

        return steps.infer_objects(), totals.infer_objects(), analogs.infer_objects(), _pairtag, step_names, step_types

    def resinreplacements(self):
        resin_replacements_path = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/data/raw/resin.xlsx'
        resin = pd.read_excel(resin_replacements_path, sheet_name=0, parse_dates=['TS'])
        resin = resin.loc[resin['UnitName'] == self.pairname]
        resin['TS'] = pd.to_datetime(resin['TS'])
        start_date = str(min(resin['TS']))
        cation_vol = int(resin['Volume'].loc[resin['ColumnType'] == 'Cation'])
        anion_vol = int(resin['Volume'].loc[resin['ColumnType'] == 'Anion'])
        return resin, start_date, cation_vol, anion_vol

class TagLists():
    def __init__(self, pairname: str):
        self.pairname = pairname
        self.dirct = 'C:/Users/pjordan/ProgramingProjects/IX_Performance_Platform/ixp_app/data/reference/reference.xlsx'

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
        tags = pd.read_excel(self.dirct, sheet_name='FullTagList', index_col='Pair')
        taglist = tags.loc[tags.index == self.pairname].values.flatten().tolist()
        tagdict = tags.loc[tags.index == self.pairname].to_dict()
        return taglist, tagdict

    # Full list of all step names for the given IX pair
    def step_names(self):
        step_names = pd.read_excel(self.dirct, sheet_name='StepNames', index_col='StepNumber')
        step_names = step_names[self.pairname]
        return step_names.rename('StepName')
    
    # Full list of all step types for the given IX pair
    def step_types(self):
        step_types = pd.read_excel(self.dirct, sheet_name='StepTypes', index_col='StepNumber')
        step_types = step_types[self.pairname]
        return step_types.rename('StepType')

class AspenDataPull():
    def __init__(self, server: str ='ARGPCS19'):
        self.aspen_server = server

    # Queries the specified Aspen server for the given tag list
    def search_aspen(self, taglist:list, query_for:str, timespan:int=5, date_limit:str='2023-01-01 00:00:00'):
        aspen_conn = AspenConn(self.aspen_server, '')
        print(f'Quering Aspen: {timespan} days back from current time')

        # if query_for == 'steps':
        #     aspen_tag_data = aspen_conn.current(taglist, days=timespan, request=5, pivot=True)
        # elif query_for == 'totals':
        #     aspen_tag_data = aspen_conn.current(taglist, days=timespan, request=5, pivot=True)
        # elif query_for == 'analogs':
        #     aspen_tag_data = aspen_conn.current(taglist, days=timespan, request=4, pivot=True)
        # else:
        #     aspen_tag_data = None

        if query_for == 'steps':
            aspen_tag_data = aspen_conn.after(taglist, date_limit=date_limit, request=5, pivot=True)
        elif query_for == 'totals':
            aspen_tag_data = aspen_conn.after(taglist, date_limit=date_limit, request=5, pivot=True)
        elif query_for == 'analogs':
            aspen_tag_data = aspen_conn.after(taglist, date_limit=date_limit, request=4, pivot=True)
        else:
            aspen_tag_data = None

        return aspen_tag_data
    