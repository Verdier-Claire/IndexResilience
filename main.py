import os
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from AgilityCompanyProduction import AgilityCompanyProduct
from LocalBackupSupplier import LocalBackupSuppliers
from VersatilityCompanyWorkforce import Versatility

Versa = Versatility()
lbs = LocalBackupSuppliers()
acp = AgilityCompanyProduct()


def main(compute=False):
    if compute:
        data_versatility = Versa.main_vcw()
        data_agitility = acp.main_acp()
        data_localbackup = lbs.main_lbs()

        data_final = data_localbackup.merge(data_versatility, on=['code'], how='left')
        data_final = data_final.merge(data_agitility, on=['code'], how='left')
        data_final.to_csv(os.getcwd() + "/data/data_out/indexResilient.csv", sep=';')

    else:
        data_final = pd.read_csv(os.getcwd() + "/data/data_out/indexResilient-100.csv", sep=';', index_col=0)
        data = data_final.iloc[:, 1:]
        print(data.describe())



if __name__ == '__main__':
    main(compute=False)  # if compute is True, compute three index, else, read index csv
