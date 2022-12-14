import pandas as pd
import os
import ast
import psycopg2


def load_data_turnover(self, df):
    df_turn = pd.read_csv(self.path_data_in + "offices-france.csv", sep=',',
                          converters={"coordinates": ast.literal_eval},
                          dtype={'siret': str})

    df_turn.sort_values('siret', ascending=True, inplace=True)
    df_turn = df_turn.iloc[:20000]
    df_turn = df_turn.merge(df[['siret', 'dest', 'qte']], on='siret', how='left')
    list_file = os.listdir(f"{self.path_data_in}data_by_siret_1")
    list_siret = [name[6:-5] for name in list_file]
    df_turn = df_turn[~df_turn['siret'].isin(list_siret)]

    return df_turn


def get_connection():
    conn = psycopg2.connect(user="iat",
                            password='bR3fTAk2VkCNbDPg',
                            host="localhost",
                            port="5433",
                            database="IndexResilience")
    return conn

def main():
    list_file = os.listdir(f"os.getcwd() + /data/data_in/data_by_siret_1")
    for file in list_file:
        df_temp = pd.read_csv(f"{os.getcwd()}/data/data_in/data_by_siret_1/{file}.csv", sep=';',
                              dtype={'siret': str, 'init_siret': str})
        siret = df_temp['init_siret'].unique()
        df_temp = df_temp[['siret', 'dist']]
        df_temp.set_index('siret', inplace=True)
        dist = df_temp[['siret', 'dist']].to_dict()




