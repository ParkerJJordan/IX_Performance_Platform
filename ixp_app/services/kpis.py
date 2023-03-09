import pandas as pd
import numpy as np
from ixp_app.services.cycles import CyclePerformance

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

class KPIs():
    def __init__(self, pairname):
        self.pairname = pairname

    def cycle_performance(self):
        ix = CyclePerformance(pairname=self.pairname)
        return