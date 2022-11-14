import os

import pandas as pd



if __name__ == '__main__':
    data_etabl = pd.read_csv(os.getcwd() +'/data/data_in/StockUniteLegale_utf8.csv', sep=',')
    data_etabl = data_etabl[data_etabl['etatAdministratifUniteLegale'] == 'A']

    siren_active = data_etabl['siren'].to_list()
    del data_etabl

    data_etabl = pd.read_csv(os.getcwd() + '/data/data_in/StockEtablissement_utf8.csv', sep=',')
    data_etabl = data_etabl[data_etabl['siren'].isin(siren_active)]

    data = pd.read_csv(os.getcwd() + "/data/data_in/turnover_french_companies_2016_2021.csv", sep=',',
                       dtype={'Siren': str})
    data_office = pd.read_csv(os.getcwd() + "/data/data_in/offices-france.csv", sep=',', dtype={'siret': str})
    data_office['Siren'] = data_office['siret'].apply(lambda row: row[:9])
    data = data.merge(data_office, on='Siren', how='inner')

    compute_turnover = data.groupby(['code'])['2016', '2017', '2018', '2019', '2020', '2021'].mean()
    count_siret = data.groupby('Siren')['Siren'].count()
    count_siren = data_etabl.groupby('siren')['siren'].count()


