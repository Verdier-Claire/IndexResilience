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
        self.num_core = multiprocessing.cpu_count() - 1


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

        data_siret_prox = self.parallelize_dataframe(data_office)

        print('finis compute proximities between siret')

        # TODO partie suivante à mettre dans des fonctions (main_lbs trop longue)

        data_siret_prox.to_csv(self.path_data_in + "siret_prox_all.csv", sep=';', index=False)

        # selection columns to concat in one data frame
        # data_siret_prox2 = data_siret_prox[['siret2', 'weight2', 'dist', 'supplier2', 'same_act2',
        #                                     'code_supplier2']].copy()

        # data_siret_prox2.rename(columns={'siret2': 'siret', 'weight2': 'weight', 'supplier2': 'supplier',
        #                                  'same_act2': 'same_activite', 'code_supplier2': 'code_supplier'}, inplace=True)
        # data_siret_prox = data_siret_prox[['siret', 'weight', 'dist', 'supplier', 'same_activite', 'code_supplier']]
        # data_siret_prox = pd.concat([data_siret_prox, data_siret_prox2])

        data_siret_prox.to_csv(self.path_data_in + "siret_prox.csv", sep=';', index=False)

        # selection only row between two companys of suppliers
        data_supplier = data_siret_prox.query("supplier == True")
        data_supplier['dist'] = data_supplier.apply(lambda row: row['dist'] * row['weight'], axis=1)
        data_supplier = data_supplier.groupby(['siret', 'code_supplier'])['dist'].sum().reset_index()

        data_rival1 = data_siret_prox.query("same_activite == True & supplier == True")
        data_rival1 = data_rival1.filter(items=['siret', 'code_supplier', 'dist'])
        data_rival1.rename(columns={'dist': 'dist_rival'}, inplace=True)
        data_rival2 = data_siret_prox.query("same_activite == True & supplier == False")
        data_rival2.reset_index(drop=True, inplace=True)
        del data_siret_prox


        # ret = self.siret_dist(data_rival2)

        ret = [[data_rival2.loc[row, 'siret'], data_rival2.loc[row, 'dist'], act] for row in data_rival2.index
                       for act in data_rival2.loc[row, 'code_supplier']]
        data_rival2 = pd.DataFrame(ret, columns=['siret', 'dist_rival', 'code_supplier'])

        data_rival = pd.concat([data_rival1, data_rival2])
        del data_rival1, data_rival2

        data_rival = data_rival.groupby(['siret', 'code_supplier'])['dist_rival'].sum().reset_index()
        data_rival = data_supplier.merge(data_rival, on=['siret', 'code_supplier'], how='left')
        data_rival['dist_rival'] = data_rival['dist_rival'].fillna(1)
        data_rival['dist'] = data_rival['dist'].fillna(0)
        del data_supplier
        data_rival['dist'] = data_rival.apply(lambda row: row['dist']/row['dist_rival'], axis=1)
        data_rival = data_rival.groupby(['siret'])['dist'].sum().reset_index()
        data_rival = data_office.merge(data_rival, on='siret')

        data_final = self.save_data(data_rival)
        print('finish to compute Local Backup Supplier')
        return data_final

    def weight_index(self, data_split, index1, data,  index2):
        # TODO fonction à vérifier
        """
        compute distance between two company.
        :param data_split:
        :param index1:.
        :param index2:
        :return: distance between two company, if one is a supplier of the other, if they are rival
        """

        # initiate values
        siret1, siret2 = data_split.loc[index1, 'siret'], data.loc[index2, 'siret']
        weight, supplier, same_act, list_same_supplier = 0, False, False, []
        # weight2, supplier2, same_act2, list_same_supplier2 = 0, False, False, []

        # compute distance between company 1 and company 2
        dist = geopy.distance.geodesic(data_split.loc[index1, 'coordinates'], data.loc[index2, 'coordinates']).km

        if dist < 1:
            dist = 1
        dist = 1 / dist

        # part of if company 2 is a supplier of company 1
        if data.loc[index2, 'code_cpf4'] in data_split.loc[index1, 'dest']:
            weight = data_split.loc[index1, 'qte'][data_split.loc[index1, 'dest'].index(data.loc[index2, 'code_cpf4'])]
            supplier = True
            same_act = data_split.loc[index1, 'code'] == data.loc[index2, 'code']
            list_same_supplier = data.loc[index2, 'code_cpf4']

        # part if company 1 and company 2 are rival (have the same suppliers)
        elif data_split.loc[index1, 'code'] == data.loc[index2, 'code'] or (list(set(data_split.loc[index1, 'dest']) &
                                                                                       set(data.loc[index2, 'dest'])) != []):
            weight = 1
            supplier = False
            same_act = True
            list_same_supplier = list(set(data_split.loc[index1, 'dest']) & set(data.loc[index2, 'dest']))

        # look if company 1 is a supplier of company 2
        # if data_split.loc[index1, 'code_cpf4'] in data.loc[index2, 'dest']:
        #     weight2 = data.loc[index2, 'qte'][data.loc[index2, 'dest'].index(data_split.loc[index1, 'code_cpf4'])]
        #     supplier2 = True
        #     same_act2 = data_split.loc[index1, 'code'] == data.loc[index2, 'code']
        #     list_same_supplier2 = data_split.loc[index1, 'code_cpf4']
#
        # # part if company 1 and company 2 are rival (have the same suppliers)
        # elif data.loc[index2, 'code'] == data_split.loc[index1, 'code'] or (list(set(data.loc[index2, 'dest']) &
        #                                                                          set(data_split.loc[index1, 'dest'])) != []):
        #     weight2 = 1
        #     supplier2 = False
        #     same_act2 = True
        #     list_same_supplier2 = list(set(data.loc[index2, 'dest']) & set(data_split.loc[index1, 'dest']))
#
        ret = [siret1, dist, weight, supplier, same_act, list_same_supplier]
        # , siret2, weight2, supplier2, same_act2, list_same_supplier2
        return ret


    def save_data(self, data):
        data.rename(columns={'dist': 'LocalBackupSuppliers'})
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
                      zip(df_split.index, df.index) if (df.loc[index2, 'code_cpf4'] in df_split.loc[index1, 'dest'] or
                                                        df_split.loc[index1, 'code'] == df.loc[index2, 'code'] or (
                                                                    list(set(df_split.loc[index1, 'dest']) &
                                                                         set(df.loc[index2, 'dest'])) != []))
                                                        ]
        data_siret_prox = pd.DataFrame(siret_prox, columns=['siret', 'dist', 'weight', 'supplier', 'same_activite',
                                                            'code_supplier'])
        return data_siret_prox
