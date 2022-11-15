import os
import pandas as pd


if __name__ == '__main__':
    # data_etabl = pd.read_csv(os.getcwd() +'/data/data_in/StockUniteLegale_utf8.csv', sep=',')
    # data_etabl = data_etabl[data_etabl['etatAdministratifUniteLegale'] == 'A']

    # siren_active = data_etabl['siren'].to_list()
    # del data_etabl

    # data_etabl = pd.read_csv(os.getcwd() + '/data/data_in/StockEtablissement_utf8.csv', sep=',')
    # data_etabl = data_etabl[data_etabl['siren'].isin(siren_active)]
    # data_etabl.to_csv(os.getcwd() + '/data/data_in/Siret_active.csv', sep=';', index=False)
    data_etabl = pd.read_csv(os.getcwd() + '/data/data_in/Siret_active.csv', sep=';', dtype={'siren': str})
    data_etabl_unique = data_etabl.groupby(['siren'])['siren'].count()
    data_etabl_unique = data_etabl_unique.to_frame().rename(columns={'siren': 'nb_siren'})
    data_etabl_unique = data_etabl_unique[data_etabl_unique['nb_siren'] == 1]
    list_siret_unique = data_etabl_unique.index
    del data_etabl_unique

    data = pd.read_csv(os.getcwd() + "/data/data_in/turnover_french_companies_2016_2021.csv", sep=',',
                       dtype={'Siren': str})
    data_office = pd.read_csv(os.getcwd() + "/data/data_in/offices-france.csv", sep=',',
                              dtype={'siret': str, 'workforce_count': int})
    data_office['Siren'] = data_office['siret'].apply(lambda row: row[:9])
    data = data.merge(data_office, on='Siren', how='inner')
    data_unique = data[data['Siren'].isin(list_siret_unique)]
    # 761 entreprises avec un seul établissement
    data_unique.loc[:,'2016_1pers'] = data_unique.apply(lambda row: row['2016']/row['workforce_count'], axis=1)
    data_unique['2017_1pers'] = data_unique.apply(lambda row: row['2017']/row['workforce_count'], axis=1)
    data_unique['2018_1pers'] = data_unique.apply(lambda row: row['2018']/row['workforce_count'], axis=1)
    data_unique['2019_1pers'] = data_unique.apply(lambda row: row['2019']/row['workforce_count'], axis=1)
    data_unique['2020_1pers'] = data_unique.apply(lambda row: row['2020']/row['workforce_count'], axis=1)
    data_unique['2021_1pers'] = data_unique.apply(lambda row: row['2021']/row['workforce_count'], axis=1)
    med_turnover = data_unique.groupby(['code'])['2016', '2017', '2018', '2019', '2020', '2021'].median()
    mean_turnover = data_unique.groupby(['code'])['2016', '2017', '2018', '2019', '2020', '2021'].mean()
    count_turnover = data_unique.groupby(['code'])['code'].count().sort_values(ascending=False)
    count_turnover.to_csv(os.getcwd() + "/data/data_out/nb_turnover_by_naf.csv", sep=';', index=True)



