import os

import numpy as np
import pandas as pd


class Turnover:
    def __init__(self):
        self.data_in = os.getcwd() + "/data/data_in/"
        self.data_out = os.getcwd() + "/data/data_out/"

    def load_data_naf(self, year):
        data_naf = pd.read_excel(self.data_in + "naf_caracteristiques_" + year + ".xlsx", header=11,
                                 na_values=['S', 'N'])
        data_naf.rename(columns={'NIVEAU NAF': 'niveau_naf', 'Effectifs salariés au 31 décembre': "Effectifs_" + year,
                                 "Secteur d'activité": "code_a732", "Chiffre d'affaires Hors Taxes": "CA_HS_" + year,
                                 "Nombre d'entreprises": 'nb_entreprises_' + year},
                        inplace=True)
        data_naf['niveau_naf'] = data_naf['niveau_naf'].str.replace(" ", "")
        data_naf['code_a732'] = data_naf['code_a732'].str.replace(" ", "")
        data_naf.query("niveau_naf == 'a732'", inplace=True)
        data_naf = data_naf.filter(items=['code_a732', "Effectifs_" + year, 'CA_HS_' + year, 'nb_entreprises_' + year])
        data_naf['code_a732'] = data_naf['code_a732'].apply(lambda row: row if row == np.nan else row[:2] +
                                                                                                  '.' + row[2:])
        return data_naf

    @staticmethod
    def merge_naf_data(data2016, data2017, data2018, data2019, data2020):
        data_merge = data2020.merge(data2019, on=['code_a732'], how='outer')
        data_merge = data_merge.merge(data2018, on=['code_a732'], how='outer')
        data_merge = data_merge.merge(data2017, on=['code_a732'], how='outer')
        data_merge = data_merge.merge(data2016, on=['code_a732'], how='outer')
        del data2020, data2019, data2018, data2017, data2016

        return data_merge

    def load_data(self):
        data_workforce = pd.read_csv(self.data_in + "workforce_french_companies_2016_2021.csv", sep=',',
                                     dtype={'Siren': str})
        data_workforce.rename(columns={'2016': 'workforce_2016', '2017': 'workforce_2017',
                                       '2018': 'workforce_2018', '2019': 'workforce_2019',
                                       '2020': 'workforce_2020', '2021': 'workforce_2021'}, inplace=True)
        dataturnover = pd.read_csv(self.data_in + "turnover_french_companies_2016_2021.csv", sep=',',
                                   dtype={'Siren': str})
        dataturnover.rename(columns={'2016': 'turnover_2016', '2017': 'turnover_2017',
                                     '2018': 'turnover_2018', '2019': 'turnover_2019',
                                     '2020': 'turnover_2020', '2021': 'turnover_2021'}, inplace=True)
        dataturnover = dataturnover.merge(data_workforce, on='Siren', how='outer')
        del data_workforce

        data_code = pd.read_csv(self.data_in + "data-coordinates.csv", sep=',', dtype={'siret': str, 'code': str})
        data_code = data_code.filter(items=['siret', 'code'])
        data_code['Siren'] = data_code['siret'].apply(lambda row: row[:9])
        data_code.rename(columns={'code': 'code_a732'}, inplace=True)

        dataturnover = dataturnover.merge(data_code, on=['Siren'], how='left')
        del data_code

        return dataturnover

    @staticmethod
    def naf_turnover_by_pers(df):
        df['CA_pers_2016'] = df['CA_HS_2016'].divide(df['Effectifs_2016'].values)
        df['CA_pers_2017'] = df['CA_HS_2017'].divide(df['Effectifs_2017'].values)
        df['CA_pers_2018'] = df['CA_HS_2018'].divide(df['Effectifs_2018'].values)
        df['CA_pers_2019'] = df['CA_HS_2019'].divide(df['Effectifs_2019'].values)
        df['CA_pers_2020'] = df['CA_HS_2020'].divide(df['Effectifs_2020'].values)

        return df

    @staticmethod
    def naf_turnover_by_company(df):
        df['CA_company_2016'] = df['CA_HS_2016'].divide(df['nb_entreprises_2016'].values)
        df['CA_company_2017'] = df['CA_HS_2017'].divide(df['nb_entreprises_2017'].values)
        df['CA_company_2018'] = df['CA_HS_2018'].divide(df['nb_entreprises_2018'].values)
        df['CA_company_2019'] = df['CA_HS_2019'].divide(df['nb_entreprises_2019'].values)
        df['CA_company_2020'] = df['CA_HS_2020'].divide(df['nb_entreprises_2020'].values)
        return df

    @staticmethod
    def workforce_by_company(df):
        df['workforce_company_2016'] = df['nb_entreprises_2016'].divide(df['Effectifs_2016'].values)
        df['workforce_company_2017'] = df['nb_entreprises_2017'].divide(df['Effectifs_2017'].values)
        df['workforce_company_2018'] = df['nb_entreprises_2018'].divide(df['Effectifs_2018'].values)
        df['workforce_company_2019'] = df['nb_entreprises_2019'].divide(df['Effectifs_2019'].values)
        df['workforce_company_2020'] = df['nb_entreprises_2020'].divide(df['Effectifs_2020'].values)
        df = df.filter(items=['code_a732', 'CA_pers_2016', 'CA_pers_2017', 'CA_pers_2018',  'CA_pers_2019',
                              'CA_pers_2020', 'CA_company_2016', 'CA_company_2017', 'CA_company_2018',
                              'CA_company_2019', 'CA_company_2020', 'workforce_company_2016',
                              'workforce_company_2017', 'workforce_company_2018', 'workforce_company_2019',
                              'workforce_company_2020'])
        return df

    def turnover_workforce_data(self, df):
        df['turnover_2016'] = df.apply(lambda row: self.nan_turnover(row['turnover_2016'],
                                                                     row['CA_company_2016']), axis=1)
        df['turnover_2017'] = df.apply(lambda row: self.nan_turnover(row['turnover_2017'],
                                                                     row['CA_company_2017']), axis=1)
        df['turnover_2018'] = df.apply(lambda row: self.nan_turnover(row['turnover_2018'],
                                                                     row['CA_company_2018']), axis=1)
        df['turnover_2019'] = df.apply(lambda row: self.nan_turnover(row['turnover_2019'],
                                                                     row['CA_company_2019']), axis=1)
        df['turnover_2020'] = df.apply(lambda row: self.nan_turnover(row['turnover_2020'],
                                                                     row['CA_company_2020']), axis=1)
        df['workforce_2016'] = df.apply(lambda row: self.nan_workforce(row['workforce_2016'],
                                                                       row['workforce_company_2016']), axis=1)
        df['workforce_2017'] = df.apply(lambda row: self.nan_workforce(row['workforce_2017'],
                                                                       row['workforce_company_2017']), axis=1)
        df['workforce_2018'] = df.apply(lambda row: self.nan_workforce(row['workforce_2018'],
                                                                       row['workforce_company_2018']), axis=1)
        df['workforce_2019'] = df.apply(lambda row: self.nan_workforce(row['workforce_2019'],
                                                                       row['workforce_company_2019']), axis=1)
        df['workforce_2020'] = df.apply(lambda row: self.nan_workforce(row['workforce_2020'],
                                                                       row['workforce_company_2020']), axis=1)
        return df

    @staticmethod
    def nan_workforce(workforce, mean_workforce):
        ret = workforce
        if ret == np.nan:
            ret = mean_workforce
        return ret

    @staticmethod
    def nan_turnover(turnover, mean_turnover):
        ret = turnover
        if ret == np.nan:
            ret = mean_turnover
        return ret





if __name__ == '__main__':
    turn = Turnover()
    data_2016 = turn.load_data_naf('2016')
    data_2017 = turn.load_data_naf('2017')
    data_2018 = turn.load_data_naf('2018')
    data_2019 = turn.load_data_naf('2019')
    data_2020 = turn.load_data_naf('2020')
    data = turn.merge_naf_data(data_2016, data_2017, data_2018, data_2019, data_2020)
    del data_2016, data_2017, data_2018, data_2019, data_2020

    data = turn.naf_turnover_by_pers(data)
    data = turn.naf_turnover_by_company(data)
    data = turn.workforce_by_company(data)

    data_turnover = turn.load_data()

    data = data.merge(data_turnover, on='code_a732', how='inner')
    data = data.filter(items=['Siren', 'siret', 'turnover_2016', 'turnover_2017', 'turnover_2018', 'turnover_2019',
                              'turnover_2020', 'turnover_2021', 'PRED2020', 'PRED2021', 'VARIA2020', 'VARIA2021',
                              'EVO20-21', 'PRED6_2022', 'EVOPRED6_2022', 'workforce_2016', 'workforce_2017',
                              'workforce_2018', 'workforce_2019', 'workforce_2020', 'workforce_2021'])

