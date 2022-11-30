import ast
import os
import geopy.distance
import pandas as pd
import numpy as np
import multiprocessing


class LocalBackupSuppliers:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"
        self.num_core = multiprocessing.cpu_count() - 10

    def load_data(self):
        data = pd.read_csv(self.path_data_in + "data-coordinates.csv", sep=';',
                           converters={'coordinates': ast.literal_eval}, dtype={'code': str})

        data_suppliers = pd.read_csv(self.path_data_in + "data_suppliers.csv", sep=';',
                                     converters={'dest': ast.literal_eval, 'qte': ast.literal_eval},
                                     dtype={'code': str})

        return data, data_suppliers

    @staticmethod
    def preprocessing(data):
        data['code_cpf4'] = data['code'].apply(lambda row: str(row[:5]))
        return data

    def main_lbs(self):
        print("begin compute lbs")
        data_office, data_suppliers = self.load_data()
        data_office = self.preprocessing(data_office)
        data_office = data_office.merge(data_suppliers, on='code_cpf4', how='left')
        del data_suppliers
        print('load data')

        data_siret_prox = self.parallelize_dataframe(data_office)

        data_siret_prox = self.compute_siret_prox(data_siret_prox, data_office)

        print('finis compute proximities between siret')

        # TODO partie suivante à mettre dans des fonctions (main_lbs trop longue)

        data_siret_prox.to_csv(self.path_data_in + "siret_prox_all.csv", sep=';', index=False)

        # selection only row between two companies of suppliers
        print('begin data supplier')
        data_supplier = data_siret_prox.query("supplier == True")
        data_supplier['dist'] = data_supplier.apply(lambda row: row['dist'] * row['weight'], axis=1)
        data_supplier = data_supplier.groupby(['siret', 'code_supplier'])['dist'].sum().reset_index()
        print("begin data competitor")
        data_rival1 = data_siret_prox.query("same_activite == True & supplier == True")
        data_rival1 = data_rival1.filter(items=['siret', 'code_supplier', 'dist'])
        data_rival1.rename(columns={'dist': 'dist_rival'}, inplace=True)
        data_rival2 = data_siret_prox.query("same_activite == True & supplier == False")
        data_rival2.reset_index(drop=True, inplace=True)
        del data_siret_prox

        ret = [[data_rival2.loc[row, 'siret'], data_rival2.loc[row, 'dist'], act] for row in data_rival2.index
               for act in data_rival2.loc[row, 'code_supplier']]
        data_rival2 = pd.DataFrame(ret, columns=['siret', 'dist_rival', 'code_supplier'])

        data_rival = pd.concat([data_rival1, data_rival2])
        del data_rival1, data_rival2

        print("sum denominator")
        data_rival = data_rival.groupby(['siret', 'code_supplier'])['dist_rival'].sum().reset_index()
        data_rival = data_supplier.merge(data_rival, on=['siret', 'code_supplier'], how='left')
        data_rival['dist_rival'] = data_rival['dist_rival'].fillna(1)
        data_rival['dist'] = data_rival['dist'].fillna(0)
        del data_supplier
        print("compute lbs")
        data_rival['dist'] = data_rival.apply(lambda row: row['dist']/row['dist_rival'], axis=1)
        data_rival = data_rival.groupby(['siret'])['dist'].sum().reset_index()
        data_rival = data_office.merge(data_rival, on='siret')

        data_final = self.save_data(data_rival)
        print('finish to compute Local Backup Supplier')
        return data_final

    @staticmethod
    def weight_index(data_split, index1, df, index2):
        """
        compute distance between two company.
        :param data_split:
        :param index1:.
        :param df:
        :param index2:
        :return: distance between two company, if one is a supplier of the other, if they are rival
        """

        # initiate values
        siret1, siret2 = data_split.loc[index1, 'siret'], df.loc[index2, 'siret']

        # compute distance between company 1 and company 2
        dist = geopy.distance.geodesic(data_split.loc[index1, 'coordinates'], df.loc[index2, 'coordinates']).km

        if dist < 1:
            dist = 1
        dist = 1 / dist

        ret = [siret1, siret2, dist]
        return ret

    @staticmethod
    def save_data(data):
        data.rename(columns={'dist': 'LocalBackupSuppliers'}, inplace=True)
        data_final = data[['siret', 'code', 'LocalBackupSuppliers']].copy()
        del data
        # data_final.to_csv(self.path_data_out + "LocalBackupSupplier.csv", sep=';', index=False)
        return data_final

    def parallelize_dataframe(self, df):
        print("begin multiprocessing ")
        num_partitions = self.num_core  # number of partitions to split dataframe
        splitted_df = np.array_split(df, num_partitions)  # split data
        # list_df = [df.drop()]

        # args = [[splitted_df[i], df, i] if i == 0
        #         else [splitted_df[i], df.drop(np.arange(0, splitted_df[i].index[-1]+1, dtype=int), axis=0), i]
        #         for i in range(0, num_partitions)
        #         ]
        args = [[splitted_df[i], df, i] for i in range(0, num_partitions)]
        pool = multiprocessing.Pool(self.num_core)
        df_pool = pool.map(self.weight_parallele, args)
        df_pool = pd.concat(df_pool)
        pool.close()
        pool.join()
        print('finish multiprocessing')
        return df_pool

    def weight_parallele(self, split_df):
        df = split_df[1]
        num_split = split_df[2]
        df_split = split_df[0]

        # compute dist between two company
        siret_prox = [self.weight_index(df_split, index1, df,  index2) for index1 in df_split.index for index2 in
                      df.index if (df.loc[index2, 'code_cpf4'] in df_split.loc[index1, 'dest'] or
                                                        df_split.loc[index1, 'code'] == df.loc[index2, 'code'] or (
                                                                    list(set(df_split.loc[index1, 'dest']) &
                                                                         set(df.loc[index2, 'dest'])) != []))
                      ]
        data_siret_prox = pd.DataFrame(siret_prox, columns=['siret', 'siret2',  'dist'])
        data_siret_prox.to_csv(self.path_data_in + "sire_prox_" + str(num_split) + ".csv", sep=';', index=False)
        print(num_split)
        return data_siret_prox

    @staticmethod
    def compute_siret_prox(data_siret_prox, df):
        print("begin compute same activite and same supplier")
        df_merge = df.copy()
        data_siret_prox = data_siret_prox.merge(df_merge, on='siret', how='left')
        df_merge.rename(columns={'siret': 'siret2', 'code_cpf4': 'code_cpf4_2', 'code': 'code_2', 'dest': 'dest_2',
                                 'qte': 'qte_2'},
                        inplace=True)
        data_siret_prox = data_siret_prox.merge(df_merge, on='siret2', how='left')
        del df_merge
        data_siret_prox['supplier'] = [True if data_siret_prox.loc[index, 'code_cpf4_2'] in data_siret_prox.loc[index,
                                                                                                                'dest']
                                       else False
                                       for index in data_siret_prox.index]
        data_siret_prox['same_activite'] = [True if (list(set(data_siret_prox.loc[index, 'dest']) &
                                                          set(data_siret_prox.loc[index, 'dest_2'])) != [])
                                            else False
                                            for index in data_siret_prox.index]
        data_siret_prox['code_supplier'] = [data_siret_prox.loc[index, 'code_cpf4_2']
                                            if data_siret_prox.loc[index, 'code_cpf4_2']
                                            in data_siret_prox.loc[index, 'dest']
                                            else list(set(data_siret_prox.loc[index, 'dest']) &
                                                      set(data_siret_prox.loc[index, 'dest_2']))
                                            for index in data_siret_prox.index]

        data_siret_prox['weight'] = [data_siret_prox.loc[index, 'qte'][data_siret_prox.loc[index,
                                                                                           'dest'].index(
            data_siret_prox.loc[index, 'code_cpf4_2'])]
                                      if data_siret_prox.loc[index, 'code_cpf4_2'] in data_siret_prox.loc[index, 'dest']
                                      else 1
                                      for index in data_siret_prox.index]

        data_siret_prox = data_siret_prox[['siret', 'weight', 'dist', 'supplier', 'same_activite', 'code_supplier']]
        print('finish feature engineering ')
        return data_siret_prox
