import os

from AgilityCompanyProduction import AgilityCompanyProduct
from LocalBackupSupplier import LocalBackupSuppliers
from VersatilityCompanyWorkforce import Versatility

Versa = Versatility()
lbs = LocalBackupSuppliers()
acp = AgilityCompanyProduct()

if __name__ == '__main__':
    data_versatility = Versa.main_vcw()
    data_agitility = acp.main_acp()
    data_localbackup = lbs.main_lbs()

    data_final = data_agitility.merge(data_versatility, on=['code'])
    data_final = data_final.merge(data_localbackup, on=['code'])
    data_final.to_csv(os.getcwd() + "/data/data_out/indexResilient.csv", sep=';')
