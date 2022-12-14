import os
import pandas as pd
from AgilityCompanyProduction import AgilityCompanyProduct
from compute_distance import DistanceBetweenCompany
from VersatilityCompanyWorkforce import Versatility
from ResilienceFlows import ResilienceSupplyFlows
Versa = Versatility()
lbs = DistanceBetweenCompany()
acp = AgilityCompanyProduct()
Resflow = ResilienceSupplyFlows()


def main(compute=False):
    if compute:
        # data_versatility = Versa.main_vcw()
        # data_agitility = acp.main_acp()
        data_localbackup = lbs.main_lbs()
        # data_resilience = Resflow.index_resilience()

        # data_final = data_localbackup.merge(data_versatility, on=['code'], how='left')
        # data_final = data_final.merge(data_agitility, on=['code'], how='left')
        # data_final = data_final.merge(data_resilience, on=['code'], how='left')

        # data_final.to_csv(os.getcwd() + "/data/data_out/indexResilient.csv", sep=';')

    else:
        data_final = pd.read_csv(os.getcwd() + "/data/data_out/indexResilient-100.csv", sep=';', index_col=0)
        data = data_final.iloc[:, 1:]
        print(data.describe())


if __name__ == '__main__':
    main(compute=True)  # compute is True if we want to compute index
