import ast
import os
import geopy.distance
import pandas as pd
import multiprocessing
import time
import numpy as np
import psycopg2
import sqlite3
from contextlib import closing

class LocalBackupSuppliers:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"
        self.num_core = multiprocessing.cpu_count() - 3

    @staticmethod
    def get_connection():
        conn = psycopg2.connect(user="iat",
                                password='bR3fTAk2VkCNbDPg',
                                host="localhost",
                                port="5433",
                                database="IndexResilience")
        return conn

    def load_data(self):
        data = pd.read_csv(self.path_data_in + "data-coordinates.csv", sep=',',
                           converters={'coordinates': ast.literal_eval},
                           dtype={'siret': str})  # ast.literal_eval

        data_suppliers = pd.read_csv(self.path_data_in + "data_suppliers.csv", sep=';',
                                     converters={'dest': ast.literal_eval, 'qte': ast.literal_eval},
                                     dtype={'siret': str})

        return data, data_suppliers

    def load_data_turnover(self):
        df_turn = pd.read_csv(self.path_data_in + "offices-france.csv", sep=',',
                              converters={"coordinates": ast.literal_eval},
                              dtype={'siret': str})
        return df_turn

    @staticmethod
    def preprocessing(data):
        data['code_cpf4'] = data['code'].apply(lambda row: str(row[:5]))
        return data

    def main_lbs(self):
        print("begin compute lbs")
        data_office, data_suppliers = self.load_data()
        data_office = self.preprocessing(data_office)
        data_office = data_office.merge(data_suppliers, on='code_cpf4', how='left')
        # data_office = data_office.sample(100, random_state=3)
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
    def save_data(data):
        data.rename(columns={'dist': 'LocalBackupSuppliers'}, inplace=True)
        data_final = data[['siret', 'code', 'LocalBackupSuppliers']].copy()
        del data
        # data_final.to_csv(self.path_data_out + "LocalBackupSupplier.csv", sep=';', index=False)
        return data_final

    def weight_parallele(self, df_turnover, df):
        coordinates = [torch.tensor(df['coordinates'][row], device='cuda') for row in df.index]
        coordinates_interet = [torch.tensor(df_turnover['coordinates'][row], device='cuda')
                               for row in df_turnover.index]
        data = [self.compute_dist_for_company(df_turnover.iloc[index], coord, df, coordinates)
                for index, coord in zip(df_turnover.index, coordinates_interet)]
    def parallelize_dataframe(self, df):
        print("begin multiprocessing ")
        df_turnover = self.load_data_turnover()
        num_partitions = self.num_core  # number of partitions to split dataframe
        df_turnover.sort_values('siret', ascending=True, inplace=True)
        df_turnover = df_turnover.iloc[:20000]
        df_turnover = df_turnover.merge(df[['siret', 'dest', 'qte']], on='siret', how='left')
        list_file = os.listdir(f"{self.path_data_in}data_by_siret_1")
        list_siret = [name[6:-5] for name in list_file]
        df_turnover = df_turnover[~df_turnover['siret'].isin(list_siret)]

        splitted_df = np.array_split(df_turnover, num_partitions)
        args = [[splitted_df[i], df] for i in range(0, num_partitions)]
        pool = multiprocessing.Pool(self.num_core)
        start = time.time()
        df_pool = pool.map(self.weight_parallele, args)
        # print(end - start)
        df_pool = pd.concat(df_pool)
        pool.close()
        pool.join()
        print('finish multiprocessing')
        end = time.time()  # 0.6286258697509766
        print(f"temps du multiprocess : {end - start}")

        return df_pool

    def weight_parallele(self, split_df):
        df = split_df[1]
        df_turnover = split_df[0]
        data = [self.dist_into_bdd(siret, coord, dest, df)
                for siret, coord, dest in zip(df_turnover['siret'], df_turnover['coordinates'], df_turnover['dest'])]
        return data

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

    def dist_into_bdd(self, siret, coord, dest, df):
        dist = self.compute_dist_for_company(siret, coord, dest, df)
        conn = self.get_connection()
        cur = conn.cursor()

        table_dist = """ CREATE TABLE IF NOT EXISTS dist_siret(
        siret VARCHAR, 
        dict_siret_dist VARCHAR);
        INSERT INTO  dist_siret(siret, dict_siret_dist)
        VALUES (%s, %s);
        """
        cur.execute(table_dist, (siret, dist))
        conn.commit()
        cur.close()

        return [siret, dist]

    @staticmethod
    def compute_dist_for_company(siret, coord, dest, df):
        start = time.time()
        dist = {df.at[row, 'siret']: geopy.distance.geodesic(coord, df.at[row, 'coordinates']).km
                for row in df.index
                if (df.at[row, 'code_cpf4'] in dest or list(set(dest) & set(df.at[row, 'dest'])) != [])}
        end = time.time()
        print(f"Time to compute distance for {siret} is {end-start}.")
        return dist




