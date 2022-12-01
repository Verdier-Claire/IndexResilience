import os

import numpy as np
import pandas as pd


class Turnover:
    def __init__(self, year=None):
        self.data_in = os.getcwd() + "/data/data_in/"
        self.data_out = os.getcwd() + "/data/data_out/"
        if year is None:
            self.list_year = ['2016', '2017', '2018', '2019', '2020']
        else:
            self.list_year = year

    def load_data_naf(self, year):
        """
        if we sum variable nb_entreprises : 4 198 538
        if we sum variable CH_HS : 3 797 140 900 000
        if we sum variable Effectifs : 13 852 275
        :param year:
        :return:
        """

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
        data_naf['code_a732'] = data_naf['code_a732'].apply(lambda row: row if row == np.nan else row[:2] + '.' +
                                                                                                  row[2:])

        data_naf["CA_HS_" + year] = data_naf["CA_HS_" + year].mul(10**6)
        return data_naf

    def preprocessing_naf(self, df_naf):
        df_filter = df_naf[['code_a732']].copy()
        for year in self.list_year:
            # compute mean turnover for one person by code naf
            ca_pers = ''.join(['CA_pers_', year])
            df_filter[ca_pers] = self.naf_turnover_by_pers(df_naf, year)
            # compute mean turnover for a company by code naf
            ca_company = ''.join(['CA_company_', str(year)])
            df_filter[ca_company] = self.naf_turnover_by_company(df_naf, year)
            # compute mean workforce for a company by code naf
            workforce_company = ''.join(['workforce_company_', str(year)])
            df_filter[workforce_company] = self.workforce_by_company(df_naf, year)
        return df_filter

    def load_data_unitelegale(self):
        data_unitelegale = pd.read_csv(self.data_in + "StockUniteLegale_utf8.csv", sep=',')
        data_unitelegale = data_unitelegale.filter(items=['siren', 'annee_CategorieEntreprise',
                                                          'anneeEffectifsUniteLegale', 'trancheEffectifsUniteLegale',
                                                          'CategorieEntreprise'])
        return data_unitelegale

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
                                     dtype={'Siren': str, '2016': np.float64, '2017': np.float64, '2018': np.float64,
                                            '2019': np.float64, '2020': np.float64})
        data_workforce.rename(columns={'2016': 'workforce_2016', '2017': 'workforce_2017',
                                       '2018': 'workforce_2018', '2019': 'workforce_2019',
                                       '2020': 'workforce_2020', '2021': 'workforce_2021'}, inplace=True)
        data_turnover = pd.read_csv(self.data_in + "turnover_french_companies_2016_2021.csv", sep=',',
                                    dtype={'Siren': str, '2016': np.float64, '2017': np.float64, '2018': np.float64,
                                           '2019': np.float64, '2020': np.float64, '2021': np.float64,
                                           'PRED2020': np.float64, 'PRED2021': np.float64, "VARIA2020": np.float64,
                                           'VARIA2021': np.float64, 'EVO20-21': np.float, 'PRED6_2022': np.float,
                                           'EVOPRED6_2022': np.float64})
        data_turnover.rename(columns={'2016': 'turnover_2016', '2017': 'turnover_2017', '2018': 'turnover_2018',
                                      '2019': 'turnover_2019', '2020': 'turnover_2020', '2021': 'turnover_2021'},
                             inplace=True)
        data_turnover = data_turnover.merge(data_workforce, on='Siren', how='outer')
        del data_workforce

        data_code = pd.read_csv(self.data_in + "data-coordinates.csv", sep=',', dtype={'siret': str, 'code': str})
        data_code = data_code.filter(items=['siret', 'code'])
        data_code['Siren'] = data_code['siret'].apply(lambda row: row[:9])
        data_code.rename(columns={'code': 'code_a732'}, inplace=True)

        data_turnover = data_turnover.merge(data_code, on=['Siren'], how='left')
        del data_code

        return data_turnover

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
        df[['workforce_2016', 'turnover_2016']] = df.apply(lambda row: self.nan_workforce(row['workforce_2016'],
                                                                                        row['workforce_company_2016'],
                                                                                        row['turnover_2016'],
                                                                                        row['CA_company_2016'],
                                                                                        row['CA_pers_2016']), axis=1)
        df[['workforce_2017', 'turnover_2017']] = df.apply(lambda row: self.nan_workforce(row['workforce_2017'],
                                                                                        row['workforce_company_2017'],
                                                                                        row['turnover_2017'],
                                                                                        row['CA_company_2017'],
                                                                                        row['CA_pers_2017']), axis=1)
        df[['workforce_2018', 'turnover_2018']] = df.apply(lambda row: self.nan_workforce(row['workforce_2018'],
                                                                                        row['workforce_company_2018'],
                                                                                        row['turnover_2018'],
                                                                                        row['CA_company_2018'],
                                                                                        row['CA_pers_2018']), axis=1)
        df[['workforce_2019', 'turnover_2019']] = df.apply(lambda row: self.nan_workforce(row['workforce_2019'],
                                                                                        row['workforce_company_2019'],
                                                                                        row['turnover_2018'],
                                                                                        row['CA_company_2019'],
                                                                                        row['CA_pers_2019']), axis=1)
        df[['workforce_2020', 'turnover_2020']] = df.apply(lambda row: self.nan_workforce(row['workforce_2020'],
                                                                                        row['workforce_company_2020'],
                                                                                        row['turnover_2020'],
                                                                                        row['CA_company_2020'],
                                                                                        row['CA_pers_2020']), axis=1)
        return df

    @staticmethod
    def nan_workforce(workforce, mean_workforce, turnover, mean_turnover, turnover_by_people):
        if (workforce != workforce) and (turnover != turnover):
            workforce = mean_workforce
            turnover = mean_turnover
        elif (workforce != workforce) and (turnover == turnover):
            workforce = turnover / turnover_by_people
        elif (workforce == workforce) and (turnover != turnover):
            turnover = workforce * turnover_by_people

        return pd.Series([workforce, turnover])

    @staticmethod
    def nan_turnover(turnover, mean_turnover):
        ret = turnover
        if ret == np.nan:
            ret = mean_turnover
        return ret

    def main_turnover(self):
        data_naf = pd.DataFrame([], columns=['code_a732'])
        for year in self.list_year:
            if year == self.list_year[0]:
                data_naf = self.load_data_naf(year)
            else:
                data_y = self.load_data_naf(year)
                data_naf = data_naf.merge(data_y, on=['code_a732'], how='outer')
                del data_y

        # compute mean turnover for one person by code naf
        data_naf = self.naf_turnover_by_pers(data_naf)
        # compute mean turnover for a company by code naf
        data_naf = self.naf_turnover_by_company(data_naf)
        # compute mean workforce for a company by code naf
        data_naf = self.workforce_by_company(data_naf)

        data_turnover = self.load_data()
        data_naf = data_naf.merge(data_turnover, on='code_a732', how='inner')

        data_naf = self.turnover_workforce_data(data_naf)

        data_naf = data_naf.filter(items=['Siren', 'siret', 'turnover_2016', 'turnover_2017', 'turnover_2018',
                                          'turnover_2019', 'turnover_2020', 'turnover_2021', 'PRED2020', 'PRED2021',
                                          'VARIA2020', 'VARIA2021', 'EVO20-21', 'PRED6_2022', 'EVOPRED6_2022',
                                          'workforce_2016', 'workforce_2017', 'workforce_2018', 'workforce_2019',
                                          'workforce_2020', 'workforce_2021'])

        return data_naf


if __name__ == '__main__':
    turn = Turnover()
    data = turn.main_turnover()









