import ast
import os
import geopy.distance
import pandas as pd
import multiprocessing
import time
import numpy as np
import psycopg2


class DistanceBetweenCompany:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"
        self.num_core = multiprocessing.cpu_count() - 2

    @staticmethod
    def get_connection(iat):
        if iat == True:
            conn = psycopg2.connect(user="iat",
                                    password='bR3fTAk2VkCNbDPg',
                                    host="localhost",
                                    port="5433",
                                    database="iat")
        else:
            conn = psycopg2.connect(user="iat",
                                password='bR3fTAk2VkCNbDPg',
                                host="localhost",
                                port="5433",
                                database="IndexResilience")
        return conn

    def load_data(self):
        data_suppliers = pd.read_csv(self.path_data_in + "data_suppliers.csv", sep=';',
                                     converters={'dest': ast.literal_eval, 'qte': ast.literal_eval},
                                     dtype={'siret': str})

        conn = self.get_connection(iat=True)
        cur = conn.cursor()

        table_siret = """SELECT siret, address.coordinates, nomenclature_activity.code FROM public.establishment
        JOIN public.address 
        ON address.id = establishment.address_id 
        JOIN public.nomenclature_activity
        ON nomenclature_activity.id = establishment.main_activity_id
        WHERE (workforce_count = 0 OR workforce_count > 10) AND address.coordinates IS NOT NULL 
        AND nomenclature_activity.code IS NOT NULL;
        """
        cur.execute(table_siret)
        data = cur.fetchall()
        conn.commit()
        cur.close()

        data = pd.DataFrame(data, columns=['siret', 'coordinates', 'code'])
        data['coordinates'] = data['coordinates'].apply(eval)

        return data, data_suppliers

    def load_data_turnover(self, df):
        df_turn = pd.read_csv(self.path_data_in + "offices-france.csv", sep=',',
                              converters={"coordinates": ast.literal_eval},
                              dtype={'siret': str})

        df_turn.sort_values('siret', ascending=True, inplace=True)
        df_turn = df_turn.iloc[:20000]
        df_turn = df_turn[~df_turn['siret'].isin(['31047151100512', '34315912500024', '00572078400106',
                                                  '34997086300024', '33052847200021', '31802333000026',
                                                  '30146504300018', '32523991100010', '33053289600033',
                                                  '31047158600019', '00572078400155', '34315938000017',
                                                  '34997183800017', '31802336300027', '30146545600038',
                                                  '32524017400012'])]

        df_turn = df_turn.merge(df[['siret', 'dest', 'qte']], on='siret', how='left')
        list_file = os.listdir(f"{self.path_data_in}data_by_siret_1")
        list_siret = [name[6:-5] for name in list_file]
        df_turn = df_turn[~df_turn['siret'].isin(list_siret)]

        # select siret already run
        conn = self.get_connection(iat=False)
        cur = conn.cursor()

        select_list_siret = """SELECT siret FROM public.dist_siret"""

        cur.execute(select_list_siret)
        siret_list = cur.fetchall()
        conn.commit()
        cur.close()
        siret_list = [item for t in siret_list for item in t]

        df_turn = df_turn[~df_turn['siret'].isin(siret_list)]

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
        del data_suppliers
        print('load data')

        self.parallelize_dataframe(data_office)
        print('finis compute distance between siret')
        return True

    def parallelize_dataframe(self, df):
        print("begin multiprocessing ")
        num_partitions = self.num_core  # number of partitions to split dataframe

        df_turnover = self.load_data_turnover(df)

        splitted_df = np.array_split(df_turnover, num_partitions)
        args = [[splitted_df[i], df] for i in range(0, num_partitions)]
        pool = multiprocessing.Pool(self.num_core)
        start = time.time()
        pool.map_async(self.weight_parallele, args)
        #df_dist = pd.concat(df_pool)
        pool.close()
        pool.join()
        print('finish multiprocessing')
        end = time.time()
        print(f"temps du multiprocess : {end - start}")

    def weight_parallele(self, split_df):
        df = split_df[1]
        df_turnover = split_df[0]
        [self.dist_into_bdd(siret, coord, dest, df)
                for siret, coord, dest in zip(df_turnover['siret'], df_turnover['coordinates'], df_turnover['dest'])]

    def dist_into_bdd(self, siret, coord, dest, df):
        dist = self.compute_dist_for_company(siret, coord, dest, df)
        conn = self.get_connection(iat=False)
        cur = conn.cursor()

        table_dist = """
        INSERT INTO  dist_siret(siret, dict_siret_dist)
        VALUES (%s, %s);
        """
        cur.execute(table_dist, (siret, str(dist)))
        conn.commit()
        cur.close()

    @staticmethod
    def compute_dist_for_company(siret, coord, dest, df):
        start = time.time()
        dist = {df.at[row, 'siret']: geopy.distance.geodesic(coord, df.at[row, 'coordinates']).km
                for row in df.index
                if (df.at[row, 'code_cpf4'] in dest or list(set(dest) & set(df.at[row, 'dest'])) != [])}
        end = time.time()
        print(f"Time to compute distance for {siret} is {end-start}.")
        return dist
