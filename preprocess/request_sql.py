import pandas as pd
import os
import psycopg2
from datetime import datetime
import psycopg2.extras as extras

class RequestSql:
    def __init__(self):
        self.path = 'C:\\Users\\OpenStudio.Aurora-R13\\Desktop\\IndexResilience'
        self.path_data_in = f"{self.path}/data/data_in"
        self.path_data_infogreffe = f"{self.path_data_in}/data_infogreffe"
        self.path_data_out = f"{self.path}/data/data_out"
        self.conn_pappers = psycopg2.connect(user="iat",
                                             password="bR3fTAk2VkCNbDPg",
                                             host="localhost",
                                             port="5432",
                                             database="pappers")

    def insert_table(self, table_sql):
        conn_index = self.conn_pappers
        cur = conn_index.cursor()
        cur.execute(table_sql)
        conn_index.commit()
        cur.close()

    def execute_values(self, df, table):
        conn = self.conn_pappers

        tuples = [tuple(x) for x in df.to_numpy()]

        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s VALUES %%s" % table
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"table : {table}")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print(f"the dataframe {table} is inserted")
        cursor.close()

    def create_table_croisement_infogreffe_2018_2022(self):

        table_index = """DROP TABLE IF EXISTS croisement_infogreffe_2018_2022;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2018_2022(Siren VARCHAR,	
        Dénomination_2018 VARCHAR, Forme_Juridique_2018 VARCHAR, Code_APE_2018 VARCHAR,	
         Code_Greffe_2018 VARCHAR, Date_immatriculation_2018 DATE,
        Date_radiation_2018 DATE, Statut_2018 VARCHAR,	Date_de_publication_2018 DATE,
        Date_de_cloture_exercice_1_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 FLOAT,
        Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Résultat_2017 FLOAT, 
        Effectif_2017 FLOAT, Date_de_cloture_exercice_2016 DATE, CA_2016 FLOAT,
        Résultat_2016 FLOAT, Effectif_2016 FLOAT, tranche_ca_millesime_2018 VARCHAR,
        tranche_ca_millesime_2017 VARCHAR, tranche_ca_millesime_2016 VARCHAR, 
        Dénomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,	
        Code_Greffe_2022 VARCHAR,	Date_immatriculation_2022 DATE,
        Date_radiation_2022 DATE, Statut_2022 VARCHAR, Date_de_publication_2022 DATE, 
        Date_de_cloture_exercice_2022 DATE, CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 FLOAT,
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT,	Résultat_2021 FLOAT,
        Effectif_2021 FLOAT, Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT,
        Resultat_2020 FLOAT, Effectif_2020 FLOAT, tranche_ca_millesime_2022 VARCHAR, tranche_ca_millesime_2021 VARCHAR,
        tranche_ca_millesime_2020 VARCHAR);
               """
        self.insert_table(table_index)

    def create_table_croisement_infogreffe_2019_2022(self):
        table_index = """DROP TABLE IF EXISTS croisement_infogreffe_2019_2022;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2019_2022(Siren VARCHAR, 
        Dénomination_2019 VARCHAR, Forme_Juridique_2019 VARCHAR, Code_APE_2019 VARCHAR,
        Code_Greffe_2019 VARCHAR, Date_immatriculation_2019 DATE,
        Date_radiation_2019 DATE, Statut_2019 VARCHAR, Date_de_publication_2019 VARCHAR, 
        Date_de_cloture_exercice_2019 DATE,	CA_2019 FLOAT, Résultat_2019 FLOAT,	Effectif_2019 FLOAT,	
        Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT,	Résultat_2018 FLOAT,
        Effectif_2018 FLOAT, Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, 
        Résultat_2017 FLOAT, Effectif_2017 FLOAT, tranche_ca_millesime_2019 VARCHAR, tranche_ca_millesime_2018 VARCHAR,
        tranche_ca_millesime_2017 VARCHAR, 
        Dénomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,
        Code_Greffe_2022 VARCHAR, Date_immatriculation_2022 DATE,
        Date_radiation_2022 DATE, Statut_2022 VARCHAR, Date_de_publication_2022 DATE,
        Date_de_cloture_exercice_2022 DATE, CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 INT,
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT, Résultat_2021 FLOAT,	Effectif_2021 INT, 
        Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT, Résultat_2020 FLOAT,	Effectif_2020 INT,
        tranche_ca_millesime_2022 VARCHAR, tranche_ca_millesime_2021 VARCHAR, tranche_ca_millesime_2020 VARCHAR);"""
        self.insert_table(table_index)

    def create_table_croisement_infogreffe_2020_2022(self):
        table_index = """DROP TABLE IF EXISTS croisement_infogreffe_2020_2022;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2020_2022(Siren VARCHAR, 
        Dénomination_2020 VARCHAR, Forme_Juridique_2020 VARCHAR, Code_APE_2020 VARCHAR,
        Code_Greffe_2020 VARCHAR, Date_immatriculation_2020 DATE,
        Date_radiation_2020 DATE, Statut_2020 VARCHAR, Date_de_publication_2020 VARCHAR, 
        Date_de_cloture_exercice_2020 DATE,	CA_2020 FLOAT, Résultat_2020 FLOAT,	Effectif_2020 FLOAT,	
        Date_de_cloture_exercice_2019 DATE, CA_2019 FLOAT, Résultat_2019 FLOAT, Effectif_2019 FLOAT,
        Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 FLOAT,
        tranche_ca_millesime_2020 VARCHAR, tranche_ca_millesime_2019 VARCHAR, tranche_ca_millesime_2017,
        Dénomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,
        Code_Greffe_2022 VARCHAR, Date_immatriculation_2022 DATE,
        Date_radiation_2022 DATE, Statut_2022 VARCHAR, Date_de_publication_2022 DATE,
        Date_de_cloture_exercice_2022 DATE, CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 FLOAT,
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT, Résultat_2021 FLOAT,	Effectif_2021 FLOAT,
        tranche_ca_millesime_2022 VARCHAR, tranche_ca_millesime_2021 VARCHAR);"""
        self.insert_table(table_index)

    def create_table_croisement_infogreffe_2021_2022(self):
        table_index = """DROP TABLE IF EXISTS croisement_infogreffe_2021_2022;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2021_2022(Siren VARCHAR,
        Dénomination_2021 VARCHAR, Forme_Juridique_2021 VARCHAR, Code_APE_2021 VARCHAR, 
                      Code_Greffe_2021 VARCHAR, 
                       Date_immatriculation_2021 DATE, Date_radiation_2021 DATE, Statut_2021 VARCHAR,
                        Date_de_publication_2021 DATE, 
                       Date_de_cloture_exercice_2021 DATE, CA_2021 DATE, Résultat_2021 FLOAT, Effectif_2021 FLOAT, 
                      Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT, 
                       Résultat_2020 FLOAT, Effectif_2020 FLOAT, Date_de_cloture_exercice_2019 DATE,
                       CA_2019 FLOAT, Résultat_2019 FLOAT, Effectif_2019 FLOAT, tranche_ca_millesime_2021 VARCHAR,
                       tranche_ca_millesime_2020 VARCHAR, tranche_ca_millesime_2019 VARCHAR, Dénomination_2022 VARCHAR,
                       Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,	
                      Code_Greffe_2022 VARCHAR,
                        Date_immatriculation_2022 DATE, Date_radiation_2022 DATE, Statut_2022 VARCHAR,
                         Date_de_publication_2022 DATE, Date_de_cloture_exercice_2022 DATE,
                       CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 FLOAT, tranche_ca_millesime_2022 VARCHAR);"""
        self.insert_table(table_index)


    def create_table_croisement_infogreffe_2018_2021(self):
        table_index = """DROP TABLE IF EXISTS croisement_infogreffe_2018_2021; 
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2018_2021(Siren VARCHAR,	
        Dénomination_2018 VARCHAR, Forme_Juridique_2018 VARCHAR, Code_APE_2018 VARCHAR,	Date_immatriculation_2018 DATE,
        Date_radiation_2018 DATE, Statut_2018 VARCHAR,	Date_de_publication_2018 DATE,
        Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 FLOAT,
        Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Résultat_2017 FLOAT, 
        Effectif_2017 FLOAT, Date_de_cloture_exercice_2016 DATE, CA_2016 FLOAT,
        Résultat_2016 FLOAT, Effectif_2016 FLOAT, tranche_ca_millesime_2018 VARCHAR,
        tranche_ca_millesime_2017 VARCHAR, tranche_ca_millesime_2016 VARCHAR, 
        Dénomination_2021 VARCHAR, Forme_Juridique_2021 VARCHAR, Code_APE_2021 VARCHAR,
        Code_Greffe_2021 VARCHAR, Date_immatriculation_2021 DATE,
        Date_radiation_2021 DATE, Statut_2021 VARCHAR, Date_de_publication_2021 DATE, 
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT, Résultat_2021 FLOAT, Effectif_2021 FLOAT,
        Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT,	Résultat_2020 FLOAT,
        Effectif_2020 FLOAT, Date_de_cloture_exercice_2019 DATE, CA_2019 FLOAT, 
        Resultat_2019 FLOAT, Effectif_2019 FLOAT, tranche_ca_millesime_2021 VARCHAR, tranche_ca_millesime_2020 VARCHAR,
        tranche_ca_millesime_2019 VARCHAR);"""
        self.insert_table(table_index)

    def create_table_turnover_french_companies(self):
        table_index = """DROP TABLE IF EXISTS turnover_french_companies;
        CREATE TABLE IF NOT EXISTS turnover_french_companies(siren VARCHAR, turnover_2016 FLOAT, 
        turnover_2017 FLOAT, turnover_2018 FLOAT, turnover_2019 FLOAT, turnover_2020 FLOAT, turnover_2021 FLOAT,
        turnover_2022 FLOAT);
               """
        self.insert_table(table_index)

    def create_table_employees_french_companies(self):
        table_index = """DROP TABLE IF EXISTS employees_french_companies; 
        CREATE TABLE IF NOT EXISTS employees_french_companies(siren VARCHAR, workforce_2016 FLOAT, 
        workforce_2017 FLOAT, workforce_2018 FLOAT, workforce_2019 FLOAT, workforce_2020 FLOAT, workforce_2021 FLOAT,
        workforce_2022 FLOAT);
               """
        self.insert_table(table_index)

    def create_table_results_french_companies(self):
        table_index = """DROP TABLE IF EXISTS results_french_companies;
        CREATE TABLE IF NOT EXISTS results_french_companies(siren VARCHAR, earning_2016 FLOAT, 
        earning_2017 FLOAT, earning_2018 FLOAT, earning_2019 FLOAT, earning_2020 FLOAT, earning_2021 FLOAT,
        earning_2022 FLOAT);"""
        self.insert_table(table_index)

    def create_table_croisement_infogreffe_2018_2019(self):
        table = f"""DROP TABLE IF EXISTS croisement_infogreffe_2018_2019;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2018_2019(Siren VARCHAR,
                       Denomination_2018 VARCHAR, Forme_Juridique_2018 VARCHAR, Code_APE_2018 VARCHAR, 
                       Adresse_2018 VARCHAR, Code_postal_2018 VARCHAR, Num_dept_2018 INT, Code_Greffe_2018 VARCHAR,
                       Date_immatriculation_2018 DATE, Date_radiation_2018 DATE, Statut_2018 VARCHAR,
                       Date_de_publication_2018 DATE, Millesime_2018 VARCHAR, Date_de_cloture_exercice_2018 DATE,
                       CA_2018 FLOAT, Resultat_2018 FLOAT, Effectif_2018 FLOAT, Millesime_2017 VARCHAR,
                       Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Resultat_2017 FLOAT, Effectif_2017 FLOAT,
                       Millesime_2016 VARCHAR, Date_de_cloture_exercice_2016 DATE, CA_2016 FLOAT, Resultat_2016 FLOAT,
                       Effectif_2016 FLOAT, tranche_ca_millesime_2018 VARCHAR, tranche_ca_millesime_2017 VARCHAR,
                       tranche_ca_millesime_2016 VARCHAR,
                       Denomination_2019 VARCHAR, Forme_Juridique_2019 VARCHAR, Code_APE_2019 VARCHAR,
                        Adresse_2019 VARCHAR, Code_postal_2019 VARCHAR, Num_dept_2019 INT, Code_Greffe_2019 VARCHAR,
                        Date_immatriculation_2019 DATE, Date_radiation_2019 DATE, Statut_2019 VARCHAR,
                       Date_de_publication_2019 DATE, Millesime_2019 VARCHAR, Date_de_cloture_exercice_2019 DATE,
                       CA_2019 FLOAT, Résultat_2019 FLOAT, Effectif_2019 FLOAT, tranche_ca_millesime_1_2019 VARCHAR);
                       """
        self.insert_table(table)

    def create_table_croisement_infogreffe_2018_2022(self):

        table = f"""DROP TABLE IF EXISTS croisement_infogreffe_complet_2022;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_complet_2022(Siren VARCHAR,
                       Denomination_2018 VARCHAR, Forme_Juridique_2018 VARCHAR, Code_APE_2018 VARCHAR, 
                       Code_Greffe_2018 VARCHAR,
                       Date_immatriculation_2018 DATE, Date_radiation_2018 DATE, Statut_2018 VARCHAR,
                       Date_de_publication_2018 DATE, Date_de_cloture_exercice_2018 DATE,
                       CA_2018 FLOAT, Resultat_2018 FLOAT, Effectif_2018 FLOAT,
                       Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Resultat_2017 FLOAT, Effectif_2017 FLOAT,
                       Date_de_cloture_exercice_2016 DATE, CA_2016 FLOAT, Resultat_2016 FLOAT,
                       Effectif_2016 FLOAT, tranche_ca_millesime_2018 VARCHAR, tranche_ca_millesime_2017 VARCHAR,
                       tranche_ca_millesime_2016 VARCHAR,
                       Denomination_2019 VARCHAR, Forme_Juridique_2019 VARCHAR, Code_APE_2019 VARCHAR,
                        Code_Greffe_2019 VARCHAR,
                        Date_immatriculation_2019 DATE, Date_radiation_2019 DATE, Statut_2019 VARCHAR,
                       Date_de_publication_2019 DATE, Date_de_cloture_exercice_2019 DATE,
                       CA_2019 FLOAT, Résultat_2019 FLOAT, Effectif_2019 FLOAT, tranche_ca_millesime_2019 VARCHAR,
                       Denomination_2020 VARCHAR, Forme_Juridique_2020 VARCHAR, Code_APE_2020 VARCHAR, 
                       Code_Greffe_2020 VARCHAR,
                       Date_immatriculation_2020 DATE, Date_radiation_2020 DATE, Statut_2020 VARCHAR,
                       Date_de_publication_2020 DATE, Date_de_cloture_exercice_2020 DATE,
                       CA_2020 FLOAT, Resultat_2020 FLOAT, Effectif_2020 FLOAT, tranche_ca_millesime_2020 VARCHAR,
                       Denomination_2021 VARCHAR, Forme_Juridique_2021 VARCHAR, Code_APE_2021 VARCHAR, 
                       Code_Greffe_2021 VARCHAR,
                       Date_immatriculation_2021 DATE, Date_radiation_2021 DATE, Statut_2021 VARCHAR,
                       Date_de_publication_2021 DATE, Date_de_cloture_exercice_2021 DATE,
                       CA_2021 FLOAT, Resultat_2021 FLOAT, Effectif_2021 FLOAT, tranche_ca_millesime_2021 VARCHAR,
                       Denomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR, 
                       Code_Greffe_2022 VARCHAR,
                       Date_immatriculation_2022 DATE, Date_radiation_2022 DATE, Statut_2022 VArCHAR,
                       Date_de_publication_2022 DATE, Date_de_cloture_exercice_2022 DATE,
                       CA_2022 FLOAT, Resultat_2022 FLOAT, Effectif_2022 FLOAT, tranche_ca_millesime_1_2022 VARCHAR);"""

        self.insert_table(table)

    def create_table_croisement_infogreffe_2019_2020(self):
        table_index = """DROP TABLE IF EXISTS croisement_infogreffe_2019_2020;
        CREATE TABLE IF NOT EXISTS croisement_infogreffe_2019_2020(Siren VARCHAR, Dénomination_2019 VARCHAR,
         Forme_Juridique_2019 VARCHAR, Code_APE_2019 VARCHAR,
                       Code_Greffe_2019 VARCHAR, 
                       Date_immatriculation_2019 DATE, Date_radiation_2019 DATE, Statut_2019 VARCHAR, 
                       Date_de_publication_2019 DATE, Date_de_cloture_exercice_2019 DATE,
                       CA_2019 FLOAT, Résultat_2019 FLOAT, Effectif_2019 FLOAT, 
                        Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 FLOAT,
                        Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Résultat_2017 FLOAT,
                        Effectif_2017 FLOAT, tranche_ca_millesime_2019 VARCHAR, tranche_ca_millesime_2018 VARCHAR,
                        tranche_ca_millesime_2017 VARCHAR, Dénomination_2020 VARCHAR, Forme_Juridique_2020 VARCHAR,
                        Code_APE_2020 VARCHAR,
                        Code_Greffe_2020 VARCHAR, Date_immatriculation_2020 DATE, Date_radiation_2020 DATE,
                        Statut_2020 VARCHAR, Date_de_publication_2020 DATE,
                        Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT, Résultat_2020 FLOAT, Effectif_2020 FLOAT,
                        tranche_ca_millesime_2020 VARCHAR);"""
        self.insert_table(table_index)