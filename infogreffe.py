import pandas as pd
import os
import numpy as np

def infogreffe():
    data_infogreffe = pd.read_csv("data/data_in/infogreffe_2018_2021.csv", sep=';',
                                  na_values='NR',
                                  dtype={'SIRET': str, 'SIREN': str})
    data_infogreffe = data_infogreffe[['SIREN', 'SIRET', 'Code APE', 'CA_2016', 'CA_2017', 'CA_2018', 'CA_2019',
                                       'CA_2020', 'Effectif_2016', 'Effectif_2017', 'Effectif_2018', 'Effectif_2019',
                                       'Effectif_2020']]
    data_infogreffe.rename(columns={'Code APE': 'code'}, inplace=True)
    data_infogreffe = data_infogreffe.fillna(-1)
    # data_infogreffe.dropna(inplace=True)
    data_infogreffe = data_infogreffe.astype({'CA_2016': int, 'CA_2017': int, 'CA_2018': int, 'CA_2019': int,
                                              'CA_2020': int, 'Effectif_2016': int, 'Effectif_2017': int,
                                              'Effectif_2018': int, 'Effectif_2019': int, 'Effectif_2020': int})
    print('load data info greffe')

    data_etabl = pd.read_csv(os.getcwd() + '/data/data_in/Siret_active.csv', sep=';', dtype={'siren': str})
    data_etabl_unique = data_etabl.groupby(['siren'])['siren'].count()
    data_etabl_unique = data_etabl_unique.to_frame().rename(columns={'siren': 'nb_siren'})
    data_etabl_unique = data_etabl_unique[data_etabl_unique['nb_siren'] == 1]
    list_siret_unique = data_etabl_unique.index
    del data_etabl_unique
    print('compute list of unique siret')

    data_unique = data_infogreffe[data_infogreffe['SIREN'].isin(list_siret_unique)]
    # 761 entreprises avec un seul établissement

    data_unique.loc[:, '2016_pers'] = data_unique.apply(lambda row: ca_by_pers(row['CA_2016'], row['Effectif_2016']),
                                                        axis=1)
    data_unique.loc[:, '2017_pers'] = data_unique.apply(lambda row: ca_by_pers(row['CA_2017'], row['Effectif_2017']),
                                                        axis=1)
    data_unique.loc[:, '2018_pers'] = data_unique.apply(lambda row: ca_by_pers(row['CA_2018'], row['Effectif_2018']),
                                                        axis=1)
    data_unique.loc[:, '2019_pers'] = data_unique.apply(lambda row: ca_by_pers(row['CA_2019'], row['Effectif_2019']),
                                                        axis=1)
    data_unique.loc[:, '2020_pers'] = data_unique.apply(lambda row: ca_by_pers(row['CA_2020'], row['Effectif_2020']),
                                                        axis=1)

    med_turnover = data_unique.groupby(['code'])['2016_pers', '2017_pers', '2018_pers', '2019_pers', '2020_pers'].\
        agg(np.nanmedian)
    # mean_turnover: object = data_unique.groupby(['code'])['2016_pers', '2017_pers', '2018_pers', '2019_pers', '2020_pers'].\
    # *    agg(np.nanmean)
    count_turnover = data_unique.groupby(['code'])['code'].count().to_frame(name='count_code').reset_index().query("count_code > 3")
    count_turnover.to_csv(os.getcwd() + "/data/data_out/nb_turnover_by_naf.csv", sep=';', index=True)



def ca_by_pers(ca, nb):
    if ca == -1:
        ret = np.nan
    elif nb == -1:
        ret = np.nan
    elif nb == 0:
        ret = np.nan
    else:
        ret = ca/nb
    return ret