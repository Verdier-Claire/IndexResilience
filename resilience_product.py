import numpy as np
import pandas as pd

from preprocess.request_sql import RequestSql

# TODO test for year 2022
class Resilience:

    def read_data(self, year):
        data_naf_cpf = RequestSql().select_naf_cpf()
        data_export = RequestSql().select_data_export(year)
        data_import = RequestSql().select_data_import(year)

        data_import['valeur_en_euros'] = data_import['valeur_en_euros'].astype(float)
        data_export['valeur_en_euros'] = data_export['valeur_en_euros'].astype(float)
        data_import['masse_kg'] = data_import['masse_kg'].astype(float)
        data_export['masse_kg'] = data_export['masse_kg'].astype(float)
        return data_export, data_import, data_naf_cpf

    def compute_efficiency(self, data_export_value, data_import_value, code):
        pd.set_option('display.float_format', lambda x: '%.36f' % x)
        # the total exports leaving country
        total_export_value = data_export_value.groupby('naf')[['valeur_en_euros', 'masse_kg']].sum()\
            .reset_index(names='naf').rename(columns={'valeur_en_euros': 'export_total_valeur_en_euros',
                                                       'masse_kg': 'export_total_masse_kg'})
        data_export_value = data_export_value.merge(total_export_value, on='naf')


        # the total imports entering country
        total_import_value = data_import_value.groupby('naf')[['valeur_en_euros', 'masse_kg']].sum()\
            .reset_index(names='naf').rename(columns={'valeur_en_euros': 'import_total_valeur_en_euros',
                                                       'masse_kg': 'import_total_masse_kg'})

        data_export_value = data_export_value.merge(total_import_value, on='naf')
        del total_import_value

        # the sum af all exports in the system
        total_all_export = data_export_value['valeur_en_euros'].sum() + data_import_value['valeur_en_euros'].sum()
        del total_export_value

        # efficiency
        data_export_value['efficiency'] = data_export_value.apply(lambda row: self.compute_efficiency_by_pays(row["pays"],row["naf"]
            ,row['valeur_en_euros'], row['export_total_valeur_en_euros'], row['import_total_valeur_en_euros'],
            total_all_export), axis=1)

        efficiency = data_export_value.groupby(code)[['efficiency']].sum().reset_index(names=[code])
        return efficiency

    def compute_redundancy(self, data_export_value, data_import_value, code):
        # the total exports leaving country
        pd.set_option('display.float_format', lambda x: '%.3f' % x)
        total_export_value = data_export_value.groupby('naf')[['valeur_en_euros', 'masse_kg']].sum()\
            .reset_index(names='pays').rename(columns={'valeur_en_euros': 'export_total_valeur_en_euros',
                                                       'masse_kg': 'export_total_masse_kg'})
        data_export_value = data_export_value.merge(total_export_value, on='naf')


        # the total imports entering country
        total_import_value = data_import_value.groupby('naf')[['valeur_en_euros', 'masse_kg']].sum()\
            .reset_index(names='naf').rename(columns={'valeur_en_euros': 'import_total_valeur_en_euros',
                                                       'masse_kg': 'import_total_masse_kg'})

        data_export_value = data_export_value.merge(total_import_value, on='naf')
        del total_import_value

        # the sum af all exports in the system
        total_all_export = data_export_value['valeur_en_euros'].sum() + data_import_value['valeur_en_euros'].sum()
        del total_export_value

        # redundancy
        data_export_value['redundancy'] = data_export_value.apply(lambda row: self.compute_redundunacy_by_pays(
            row['valeur_en_euros'], row['export_total_valeur_en_euros'], row['import_total_valeur_en_euros'],
            total_all_export), axis=1)

        # efficiency
        data_export_value['efficiency'] = data_export_value.apply(lambda row: self.compute_efficiency_by_pays(row["pays"],
                                                                                                              row["naf"],
            row['valeur_en_euros'], row['export_total_valeur_en_euros'], row['import_total_valeur_en_euros'],
            total_all_export), axis=1)

        redundancy = data_export_value.groupby(code)[['redundancy']].sum().reset_index(names=[code])
        redundancy['redundancy'] = - redundancy['redundancy']
        return redundancy

    def compute_resilience(self, data_export_value, data_import_value, code='cpf'):
        efficiency = self.compute_efficiency(data_export_value, data_import_value, code)
        redundancy = self.compute_redundancy(data_export_value, data_import_value, code)

        alpha = efficiency.merge(redundancy, on=code)
        del efficiency, redundancy

        alpha['alpha'] = alpha.apply(lambda row: row['efficiency'] / (row['efficiency'] + row['redundancy']), axis=1)

        alpha['resilience'] = alpha.apply(lambda row: - row['alpha'] * np.log10(row['alpha']), axis=1)
        return alpha

    def main(self):
        pd.set_option('display.float_format', lambda x: '%.36f' % x)
        leontief_matrix = self.compute_leontief_matrix()
        print("compute resilience product by code naf")
        df_resilience_naf = pd.concat([self.compute_resilience_by_naf(year, leontief_matrix)
                                       for year in range(2013, 2023, 1)])
        print("compute resilience product by code cpf")
        df_resilience_hs4 = pd.concat([self.compute_resilience_by_hs4(year, leontief_matrix)
                                       for year in range(2013, 2023, 1)])


        RequestSql().table_indicateur_resilience_naf(df_resilience_naf)
        RequestSql().table_indicateur_resilience_naf(df_resilience_hs4)



    def compute_leontief_matrix(self):
        data_consume_nace = RequestSql().select_iot_consume_nace()
        data_consume_nace = data_consume_nace.pivot(index='dest', columns='nace', values='qte')
        data_consume_nace.drop(index='W-Adj', inplace=True)

        # add columns not in columns but in index
        a = [col for col in data_consume_nace.index if col not in data_consume_nace.columns]
        data_consume_nace[a] = 0
        data_consume_nace.fillna(0, inplace=True)

        # to have same sort between code in index and code in columns
        data_consume_nace = data_consume_nace.reindex(sorted(data_consume_nace.columns), axis=1)
        data_consume_nace = data_consume_nace.reindex(sorted(data_consume_nace.index), axis=1)

        # type float
        data_consume_nace = data_consume_nace.astype(float)
        # compute leontief matrix
        n = data_consume_nace.__len__()
        leontief_matrix = np.linalg.inv(np.identity(n) - data_consume_nace)

        df_leontief = pd.DataFrame(leontief_matrix, columns=data_consume_nace.columns, index=data_consume_nace.index)
        print("compute leontief matrix")
        return df_leontief

    def compute_redundunacy_by_pays(self, export_value, total_export_value, total_import_value, all_export_value):
        ret = np.log10((export_value * export_value) / (total_export_value * total_import_value))
        weight = export_value / all_export_value

        ret = weight * ret
        return ret

    def compute_efficiency_by_pays(self,pays, cpf ,export_value, total_export_value, total_import_value, all_export_value):
        ratio = (export_value * all_export_value) / (total_export_value * total_import_value)
        if ratio < 1:
            print("coucou")
        ret = np.log10(ratio)
        weight = export_value / all_export_value
        ret = weight * ret
        return ret

    def compute_resilience_by_hs4(self, year, leontief_matrix):
        year_str = str(year)
        data_export, data_import, data_naf_cpf = self.read_data(year_str)

        df_resilience = self.compute_resilience(data_export, data_import)
        df_resilience.fillna(0, inplace=True)
        df_resilience.drop_duplicates(inplace=True)
        # test = leontief_matrix[leontief_matrix.index.isin(df_resilience['naf'].to_list())]
        # test = test[test.index.to_list()]
        # df_resilience = df_resilience.loc[df_resilience['cpf'].isin(test.index.to_list())]
        # test = test.multiply(df_resilience['resilience'].to_list(), axis='index')
        # coeff_leontief = pd.DataFrame(test.sum(axis=1), columns=["coefficient_leontief"]).reset_index()
        # coeff_leontief.rename(columns={"nace": "naf"}, inplace=True)

        df_resilience = df_resilience.merge(data_naf_cpf, on='cpf')
        df_resilience = df_resilience.groupby('naf').sum().reset_index()

        test = leontief_matrix[leontief_matrix.index.isin(df_resilience['naf'].to_list())]
        test = test[test.index.to_list()]
        df_resilience = df_resilience.loc[df_resilience['naf'].isin(test.index.to_list())]
        test = test.multiply(df_resilience['resilience'].to_list(), axis='index')
        coeff_leontief = pd.DataFrame(test.sum(axis=0), columns=["coefficient_leontief"]).reset_index(names='naf')

        df_resilience = df_resilience.merge(coeff_leontief, on='naf')
        df_resilience['year'] = year

        return df_resilience

    def compute_resilience_by_naf(self, year, leontief_matrix):
        year_str = str(year)
        data_export, data_import, data_naf_cpf = self.read_data(year_str)

        data_export.drop(columns='naf', inplace=True)
        data_import.drop(columns='naf', inplace=True)

        data_export = data_export.merge(data_naf_cpf, on='cpf')
        data_import = data_import.merge(data_naf_cpf, on='cpf')

        data_export = data_export.groupby(['pays', 'naf']).sum().reset_index()
        data_import = data_import.groupby(['pays', 'naf']).sum().reset_index()

        df_resilience = self.compute_resilience(data_export, data_import, 'naf')
        df_resilience.fillna(0, inplace=True)
        df_resilience.drop_duplicates(inplace=True)

        test = leontief_matrix[leontief_matrix.index.isin(df_resilience['naf'].to_list())]
        test = test[test.index.to_list()]
        df_resilience = df_resilience.loc[df_resilience['naf'].isin(test.index.to_list())]
        test = test.multiply(df_resilience['resilience'].to_list(), axis='index')
        coeff_leontief = pd.DataFrame(test.sum(axis=0), columns=["coefficient_leontief"]).reset_index(names='naf')

        df_resilience = df_resilience.merge(coeff_leontief, on='naf')
        df_resilience['year'] = year

        return df_resilience






if __name__ == "__main__":
    Resilience().main()