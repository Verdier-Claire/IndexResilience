import pandas as pd
import numpy as np
import psycopg2
import os
import ast
import time
import multiprocessing


class LocalBackupSupplier:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"
        self.num_core = multiprocessing.cpu_count() - 2

    @staticmethod
    def get_connection(iat):
        if iat == True:
            conn = psycopg2.connect(user="iat",
                                    password="bR3fTAk2VkCNbDPg",
                                    host="localhost",
                                    port="5433",
                                    database="iat")
        else:
            conn = psycopg2.connect(user="iat",
                                    password="bR3fTAk2VkCNbDPg",
                                    host="localhost",
                                    port="5433",
                                    database="IndexResilience")
        return conn

    def load_from_sql(self):
        conn = self.get_connection(iat=False)
        cur = conn.cursor()
        table_dist = """SELECT siret, dict_siret_dist FROM dist_siret
        LIMIT 20 OFFSET 480;"""
        cur.execute(table_dist)
        data = cur.fetchall()
        data = pd.DataFrame(data, columns=['siret', 'dict_siret'])
        cur.close()
        conn = self.get_connection(iat=True)
        cur = conn.cursor()
        table_data = """SELECT siret, nomenclature_activity.code FROM public.establishment
        JOIN public.nomenclature_activity
        ON nomenclature_activity.id = establishment.main_activity_id
        WHERE nomenclature_activity.code IS NOT NULL;
        """
        cur.execute(table_data)
        data_act = cur.fetchall()
        data_act = pd.DataFrame(data_act, columns=['siret', 'code'])
        cur.close()
        return data, data_act

    def load_from_csv(self):
        data_suppliers = pd.read_csv(f"{self.path_data_in}data_suppliers.csv", sep=';',
                                     converters={'dest': ast.literal_eval, 'qte': ast.literal_eval},
                                     dtype={'code_cpf4': str})
        return data_suppliers

    @staticmethod
    def preprocessing(df, df_act):
        df['dict_siret'] = df['dict_siret'].apply(eval)
        df_act['code_cpf4'] = df_act['code'].apply(lambda row: row[:5])
        return df, df_act

    @staticmethod
    def merge_data(df, df_act, df_suppliers):
        df = df.merge(df_act, on=['siret'])
        df = df.merge(df_suppliers, on=['code_cpf4'])
        return df

    def compute_index(self, args):
        df = args[0]
        df_act = args[1]
        df_suppliers = args[2]
        [self.compute_index_row(df.at[row, 'dict_siret'], df.at[row, 'dest'], df.at[row, 'qte'],
                                df_act, df_suppliers, df.at[row, 'siret'])
         for row in df.index]

    def compute_index_row(self, dict_siret, list_dest, list_qte, df_act, df_suppliers, siret):
        start = time.time()
        data_row = pd.DataFrame.from_dict(dict_siret, orient='index', columns=['dist']).reset_index(names='siret')
        data_row = data_row.merge(df_act, on=['siret'])
        data_row = data_row.merge(df_suppliers, on=['code_cpf4'])
        data_row['dist'] = 1/data_row['dist']

        data_result = [[data_row.at[row, 'dist'],
                        self.weight_numerator(data_row.at[row, 'code_cpf4'], list_dest, list_qte),
                        data_row.at[row, 'code_cpf4'] in list_dest,
                        list(set(data_row.at[row, 'dest']) & set(list_dest)) != [],
                        data_row.at[row, 'code_cpf4']]
                       for row in data_row.index]
        data_result = pd.DataFrame(data_result, columns=['dist', 'weight', 'num', 'den', 'code_cpf4'])
        data_result['dist'].replace(np.inf, 1000, inplace=True)
        numerateur = data_result.query("num == True")

        numerateur['dist_weight'] = numerateur['dist'].mul(numerateur['weight'])
        num = numerateur.groupby(['code_cpf4'])['dist_weight'].sum().reset_index()

        # numerateur = numerateur['dist'].mul(numerateur['weight']).groupby('code_cpf4').sum()
        denominateur = data_result[data_result['den']==True]
        den = denominateur.groupby(['code_cpf4'])['dist'].sum().reset_index()
        den['dist'].replace(0, 1, inplace=True)
        data_temp = num[['dist_weight', 'code_cpf4']].merge(den[['dist', 'code_cpf4']], on=['code_cpf4'])
        result = data_temp['dist_weight'].div(data_temp['dist']).sum()

        # if denominateur == 0:
        #    denominateur = 1
        # result = numerateur / denominateur
        end = time.time()
        print(f"Finish to compute lbs ({result})for {siret} in {(end - start)/60}")

        conn = self.get_connection(iat=False)
        cur = conn.cursor()
        table_index = """CREATE TABLE IF NOT EXISTS localbackupsupplier(siret VARCHAR, lbs FLOAT);
        INSERT INTO localbackupsupplier(siret, lbs)
        VALUES (%s, %s);
        """
        cur.execute(table_index, (siret, result))
        conn.commit()
        cur.close()

    def parallelize_dataframe(self, df, df_act, df_suppliers):
        num_partitions = self.num_core
        splitted_df = np.array_split(df, num_partitions)
        args = [[splitted_df[i], df_act, df_suppliers] for i in range(0, num_partitions)]
        pool = multiprocessing.Pool(self.num_core)
        pool.map_async(self.compute_index, args)
        pool.close()
        pool.join()

    def main(self):
        print("begin compute index Local Backup Supplier")
        data, data_act = self.load_from_sql()
        print("finish load sql data")
        data_suppliers = self.load_from_csv()
        print("finish load csv data")
        data, data_act = self.preprocessing(data, data_act)
        print("finish preprocessing data")
        data = self.merge_data(data, data_act, data_suppliers)
        print("merge three data and begin compute index")

        self.parallelize_dataframe(data, data_act, data_suppliers)
        del data, data_act, data_suppliers

    @staticmethod
    def weight_numerator(cpf4, list_dest, qte):
        if cpf4 in list_dest:
            weight = qte[list_dest.index(cpf4)]
        else:
            weight = 1
        return weight


if __name__ == "__main__":
    lbs = LocalBackupSupplier()
    lbs.main()
