import os
import geopy.distance
import pandas as pd
import numpy as np
import itertools
import multiprocessing

class LocalBackupSuppliers:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"

    def load_data(self):
        data = pd.read_csv(self.path_data_in + "data-coordinates.csv", sep=',')
        data['coordinates'].fillna(data['coordinates-2'], inplace=True)
        data.drop(['coordinates-2'], inplace=True, axis=1)
        data['code_cpf4'] = data['code'].apply(lambda row: str(row[:5]))

        data_suppliers = pd.read_csv(self.path_data_in + "data_suppliers.csv", sep=';')
        data = data.merge(data_suppliers, on='code_cpf4', how='left')
        del data_suppliers
        data.dropna(inplace=True)
        data.drop_duplicates(inplace=True)
        data = data.sample(500, random_state=3)
        data.reset_index(inplace=True, drop=True)
        return data

    def preprocessing(self, data):
        data['coordinates'] = data['coordinates'].apply(eval)
        data['dest'] = data['dest'].apply(eval)
        data['qte'] = data['qte'].apply(eval)
        return data


    def main_lbs(self):
        print("begin compute lbs")
        data_office = self.load_data()
        data_office = self.preprocessing(data_office)
        print('load data')

        siret_prox = self.parallelize_dataframe(data_office)

        # siret_prox = [self.weight_index(data_office, index1, index2) for index1, index2 in
        #               itertools.permutations(data_office.index, 2)]

        print('finis compute proximities between siret')

        data_siret_prox = pd.DataFrame(siret_prox, columns=['siret', 'weight', 'dist', 'supplier', 'same_activite',
                                                            'code_supplier'])

        data_supplier = data_siret_prox.query("supplier == 'True'")
        data_supplier['dist'] = data_supplier['dist', 'weight'].apply(lambda row: row['dist'] * row['weight'], axis=1)
        data_supplier = data_supplier.groupby(['siret', 'code_supplier'])['dist'].sum().reset_index()

        data_rival = data_siret_prox.query("same_activite == 'True'")
        del data_siret_prox
        data_rival = [[data_rival.loc[index, 'siret'], act, data_rival.loc[index, 'dist']] for index in data_rival.index
                      for act in data_rival.loc[index, 'code_supplier']]
        data_rival = pd.DataFrame(data_rival, columns=['siret', 'code_supplier', 'dist_rival']).groupby(['siret',
                                                                                                   'code_supplier'
                                                                                                   ])['dist_rival'].\
            sum().reset_index()
        data_rival = data_supplier.merge(data_rival, on=['siret', 'code_supplier'], how='left')
        data_rival['dist_rival'] = data_rival['dist_rival'].fillna(1)
        data_rival['dist'] = data_rival['dist'].fillna(0)
        del data_supplier
        data_rival['dist'] = data_rival.apply(lambda row: row['dist']/row['dist_rival'], axis=1)
        data_rival = data_rival.groupby(['siret'])['dist'].sum().reset_index()

        data_final = self.save_data(data_rival)
        print('finish to compute Local Backup Supplier')
        return data_final

    def weight_index(self, data_cut, index1, data, index2):
        siret = data_cut.loc[index1, 'siret']
        dist = geopy.distance.geodesic(data_cut.loc[index1, 'coordinates'], data.loc[index2, 'coordinates']).km
        if dist < 1:
            dist = 1
        dist = 1 / dist
        if data.loc[index2, 'code_cpf4'] in data.loc[index1, 'dest']:
            weight = data_cut.loc[index1, 'qte'][data_cut.loc[index1, 'dest'].index(data_cut.loc[index2, 'code'])]
            supplier = True
            same_act = data_cut.loc[index1, 'code'] == data_cut.loc[index2, 'code']
            list_same_supplier = data_cut.loc[index2, 'code']
            return [siret, weight, dist, supplier, same_act, list_same_supplier]
        elif data_cut.loc[index1, 'code'] == data_cut.loc[index2, 'code'] or (list(set(data_cut.loc[index1, 'dest']) &
                                                                                   set(data_cut.loc[index2, 'dest'])) != []):
            weight = 1
            dist = geopy.distance.geodesic(data_cut.loc[index1, 'coordinates'], data_cut.loc[index2, 'coordinates']).km
            supplier = False
            same_act = True
            list_same_supplier = list(set(data_cut.loc[index1, 'dest']) & set(data_cut.loc[index2, 'dest']))
            return [siret, weight, dist, supplier, same_act, list_same_supplier]


    def save_data(self, data):
        data_final = data[['siret', 'code', 'LocalBackupSuppliers']]
        # data_final.to_csv(self.path_data_out + "LocalBackupSupplier.csv", sep=';', index=False)
        return data_final

    def parallelize_dataframe(self, df):
        num_partitions = self.num_core  # number of partitions to split dataframe
        splitted_df = np.array_split(df, num_partitions)
        args = [[splitted_df[i], df] for i in range(0, num_partitions)]
        pool = multiprocessing.Pool(self.num_core)
        df_pool = pool.map(self.weight_parallele, args)
        df_pool = pd.concat(df_pool)
        pool.close()
        pool.join()
        return df_pool

    def weight_parallele(self, split_df):
        df = split_df[1]
        df_split = split_df[0]
        siret_prox = [self.weight_index(df_split, index1, df,  index2) for index1, index2 in
                      zip(df_split.index, df.index)]
        data_siret_prox = pd.DataFrame(siret_prox, columns=['siret', 'dist', 'weight', 'supplier', 'same_activite',
                                                            'code_supplier', 'siret2', 'weight2', 'supplier2',
                                                            'same_act2', 'code_supplier2'])
        return data_siret_prox
