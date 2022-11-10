import os

import pandas as pd

if __name__ == '__main__':
    data_etabl = pd.read_csv(os.getcwd() +'/data/data_in/StockUniteLegale_utf8.csv', sep=',')
    data_etabl = data_etabl[data_etabl['etatAdministratifUniteLegale'] == 'A']

    siren_active = data_etabl['siren'].to_list()
    del data_etabl

    data_etabl = pd.read_csv(os.getcwd() + '/data/data_in/StockEtablissement_utf8.csv', sep=',')
    data_etabl = data_etabl[data_etabl['siren'].isin(siren_active)]