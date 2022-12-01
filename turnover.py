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
                                            '2019': np.float64, '2020': np.float64, '2021': np.float64},
                                     na_values=["nan", "#DIV/0!"])
        data_workforce = data_workforce.set_index(['Siren']).add_prefix("workforce_").reset_index()

        data_turnover = pd.read_csv(self.data_in + "turnover_french_companies_2016_2021.csv", sep=',',
                                    dtype={'Siren': str, '2016': np.float64, '2017': np.float64, '2018': np.float64,
                                           '2019': np.float64, '2020': np.float64, '2021': np.float64,
                                           'PRED2020': np.float64, 'PRED2021': np.float64, 'VARIA2020': np.float64,
                                           'VARIA2021': np.float64, "EVO20-21": np.float64, "PRED6_2022": np.float64,
                                           "EVOPRED6_2022": np.float64},
                                    na_values=["nan", "#DIV/0!"])
        data_turnover.rename(columns=lambda col_name: f"turnover_{col_name}" if col_name in {"2016", "2017", "2018",
                                                                                             "2019", "2020", "2021"}
        else col_name, inplace=True)

        data_code = pd.read_csv(self.data_in + "data-coordinates.csv", sep=',', dtype={'siret': str, 'code': str})

        return data_turnover, data_workforce, data_code

    def preprocessing(self, data_turnover, data_workforce, data_code):
        data_workforce = self.workforce_2016_false_values(data_workforce)

        data_turnover, data_workforce = self.siren_len_9(data_turnover, data_workforce)

        data_code = data_code.filter(items=['siret', 'code'])
        data_code['Siren'] = data_code['siret'].apply(lambda row: str(row[:9]))
        data_code.rename(columns={'code': 'code_a732'}, inplace=True)

        return data_turnover, data_workforce, data_code

    def merge_data(self, data_turnover, data_workforce, data_code):
        data_turnover = data_turnover.merge(data_workforce, on='Siren', how='outer')
        data_turnover = data_turnover.merge(data_code, on=['Siren'], how='left')
        data_turnover.fillna(np.nan, inplace=True)

        return data_turnover

    @staticmethod
    def workforce_2016_false_values(data_workforce):
        data_workforce.loc[data_workforce['workforce_2016'] == 7906000, 'workforce_2016'] = 7906
        data_workforce.loc[data_workforce['workforce_2016'] == 728000, 'workforce_2016'] = 728
        data_workforce.loc[data_workforce['workforce_2016'] == 390000, 'workforce_2016'] = 390
        data_workforce.loc[data_workforce['workforce_2016'] == 317000, 'workforce_2016'] = 317
        data_workforce.loc[data_workforce['workforce_2016'] == 308000, 'workforce_2016'] = 308
        data_workforce.loc[data_workforce['workforce_2016'] == 208000, 'workforce_2016'] = 208
        data_workforce.loc[data_workforce['workforce_2016'] == 205000, 'workforce_2016'] = 205
        data_workforce.loc[data_workforce['workforce_2016'] == 174000, 'workforce_2016'] = 174
        data_workforce.loc[data_workforce['workforce_2016'] == 158000, 'workforce_2016'] = 158
        data_workforce.loc[data_workforce['workforce_2016'] == 119000, 'workforce_2016'] = 119
        data_workforce.loc[data_workforce['workforce_2016'] == 97000, 'workforce_2016'] = 97
        data_workforce.loc[data_workforce['workforce_2016'] == 61000, 'workforce_2016'] = 61
        data_workforce.loc[data_workforce['workforce_2016'] == 60000, 'workforce_2016'] = 60
        data_workforce.loc[data_workforce['workforce_2016'] == 58000, 'workforce_2016'] = 58
        data_workforce.loc[data_workforce['workforce_2016'] == 56000, 'workforce_2016'] = 56
        data_workforce.loc[data_workforce['workforce_2016'] == 54000, 'workforce_2016'] = 54
        data_workforce.loc[data_workforce['workforce_2016'] == 52000, 'workforce_2016'] = 52
        data_workforce.loc[data_workforce['workforce_2016'] == 50000, 'workforce_2016'] = 50
        data_workforce.loc[data_workforce['workforce_2016'] == 47000, 'workforce_2016'] = 47
        return data_workforce

    @staticmethod
    def siren_len_9(data_turnover, data_workforce):
        data_turnover['Siren'] = [("0" * (9 - len(row))) + row if (len(row) < 9) else row
                                  for row in data_turnover['Siren']]
        data_workforce['Siren'] = [("0" * (9 - len(row))) + row if len(row) < 9 else row
                                   for row in data_workforce['Siren']]
        return data_turnover, data_workforce

    @staticmethod
    def naf_turnover_by_pers(df, year):
        ca_pers = ''.join(['CA_pers_', year])
        ca_hs = ''.join(['CA_HS_', str(year)])
        workforce = ''.join(['Effectifs_', str(year)])
        df[ca_pers] = df[ca_hs].divide(df[workforce].values)
        return df[ca_pers]

    @staticmethod
    def naf_turnover_by_company(df, year):
        ca_company = ''.join(['CA_company_', str(year)])
        ca_hs = ''.join(['CA_HS_', str(year)])
        nb_company = ''.join(['nb_entreprises_', str(year)])
        df[ca_company] = df[ca_hs].divide(df[nb_company].values)
        return df[ca_company]

    @staticmethod
    def workforce_by_company(df, year):
        workforce_company = ''.join(['workforce_company_', str(year)])
        effectifs = ''.join(['Effectifs_', str(year)])
        nb_company = ''.join(['nb_entreprises_', str(year)])
        df[workforce_company] = df[effectifs].divide(df[nb_company].values)
        return df[workforce_company]

    def turnover_workforce_data(self, df):
        data = df[['Siren', 'siret', 'PRED2020', 'PRED2021', 'VARIA2020', 'VARIA2021', 'EVO20-21', 'PRED6_2022',
                   'EVOPRED6_2022', 'turnover_2021', 'workforce_2021']].copy()

        data[['workforce_2016', 'turnover_2016']] = df.apply(lambda row: self.nan_workforce(row['workforce_2016'],
                                                                                            row['workforce_company_'
                                                                                                '2016'],
                                                                                            row['turnover_2016'],
                                                                                            row['CA_company_2016'],
                                                                                            row['CA_pers_2016']),
                                                             axis=1)

        data[['workforce_2017', 'turnover_2017']] = df.apply(lambda row: self.nan_workforce(row['workforce_2017'],
                                                                                            row['workforce_company_'
                                                                                                '2017'],
                                                                                            row['turnover_2017'],
                                                                                            row['CA_company_2017'],
                                                                                            row['CA_pers_2017']),
                                                             axis=1)
        data[['workforce_2018', 'turnover_2018']] = df.apply(lambda row: self.nan_workforce(row['workforce_2018'],
                                                                                            row['workforce_company_'
                                                                                                '2018'],
                                                                                            row['turnover_2018'],
                                                                                            row['CA_company_2018'],
                                                                                            row['CA_pers_2018']),
                                                             axis=1)
        data[['workforce_2019', 'turnover_2019']] = df.apply(lambda row: self.nan_workforce(row['workforce_2019'],
                                                                                            row['workforce_company_'
                                                                                                '2019'],
                                                                                            row['turnover_2018'],
                                                                                            row['CA_company_2019'],
                                                                                            row['CA_pers_2019']),
                                                             axis=1)
        data[['workforce_2020', 'turnover_2020']] = df.apply(lambda row: self.nan_workforce(row['workforce_2020'],
                                                                                            row['workforce_company_'
                                                                                                '2020'],
                                                                                            row['turnover_2020'],
                                                                                            row['CA_company_2020'],
                                                                                            row['CA_pers_2020']),
                                                             axis=1)
        return data

    @staticmethod
    def nan_workforce(workforce, mean_workforce, turnover, mean_turnover, turnover_by_people):
        if (np.isnan(workforce)) and (np.isnan(turnover)):
            workforce = mean_workforce
            turnover = mean_turnover
        elif (np.isnan(workforce)) and (not np.isnan(turnover)):
            workforce = turnover / turnover_by_people
        elif (not np.isnan(workforce)) and (np.isnan(turnover)):
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









