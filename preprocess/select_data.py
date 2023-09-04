#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 14:37:58 2022

@author: nsauzeat
"""
import pandas as pd
import os
import psycopg2


class Infogreffe():
    def __init__(self):
        self.path = 'C:\\Users\\OpenStudio.Aurora-R13\\Desktop\\IndexResilience'
        self.path_data_in = f"{self.path}/data/data_in"
        self.path_data_infogreffe = f"{self.path_data_in}/data_infogreffe"
        self.path_data_out = f"{self.path}/data/data_out"
        self.con_index = psycopg2.connect(user="iat",
                                    password="bR3fTAk2VkCNbDPg",
                                    host="localhost",
                                    port="5432",
                                    database="pappers")

    def read_excel(self):
        list_file = {}
        for filename in os.listdir(self.path_data_infogreffe):
            if filename.endswith(".csv"):
                list_file.update({filename: pd.read_csv(self.path_data_infogreffe+"/"+filename, sep=";", header=0,
                                                        dtype={"Siren": str, 'Nic': str, 'Code postal': str, })})
        return list_file


    def sorted_companies(self, liste_data):
        i=0
        for file in liste_data.values():
            if i >0:
                try:
                    file["Siren"] = file["Siren"].astype(str)
                    file.set_index("Siren", inplace=True)
                except KeyError:
                        file.reset_index(inplace=True)
                        try:
                            file["Siren"].astype(str)
                            file["Siren"] =file["Siren"].str.split(".")[0]
                            file.set_index("Siren", inplace=True)
                        except KeyError: pass
                try:
                    file.drop(columns=["Nic", "Greffe", "Région", "Département", "Greffe", "fiche_identite", "Ville",
                                       "Département", "Région", "Geolocalisation", "Durée 2", "Durée 1", "Durée 3",
                                       "Libellé APE"],
                              inplace=True)
                except KeyError: pass
            i+=1
        data_final= liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2019.csv"],
                                                              lsuffix="_2018", rsuffix="_2019",
                                                              how="inner", on='Siren')

        data_final= data_final.join(liste_data["chiffres-cles-2020.csv"],
                                    rsuffix="_2020", how="inner", on='Siren')
        data_final= data_final.join(liste_data["chiffres-cles-2021.csv"], rsuffix="_2021",
                                    how="inner", on='Siren')
        data_final =data_final.join(liste_data["chiffres-cles-2022.csv"], rsuffix="_2022",
                                    how="inner", on='Siren')
        return data_final

    def get_first_year_comp(self, liste_data):
        data_first= liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2019.csv"],
                                                              lsuffix="_2018", rsuffix="_2019", how="inner")
        data_second = data_first.join(liste_data["chiffres-cles-2020.csv"], rsuffix="_2020", how="inner")
        return  data_first, data_second

    def get_midlle_year_comp(self, liste_data):
        data_middle_first= liste_data["chiffres-cles-2019.csv"].join(liste_data["chiffres-cles-2020.csv"],
                                                                     lsuffix="_2019", rsuffix="_2020", how="inner")
        data_middle_second= data_middle_first.join(liste_data["chiffres-cles-2021.csv"], rsuffix="_2021", how="inner")

        return data_middle_first, data_middle_second
    
    def get_2022(self, liste_data):
        info_2018_2022 = liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2018", rsuffix="_2022", how="inner")
        info_2019_2022 = liste_data["chiffres-cles-2019.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2019", rsuffix="_2022", how="inner")
        info_2020_2022 = liste_data["chiffres-cles-2020.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2020", rsuffix="_2022", how="inner")
        info_2021_2022 = liste_data["chiffres-cles-2021.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2021", rsuffix="_2022", how="inner")
        info_2018_2022.to_excel(self.path_data_out + "croisement_infogreffe_2018_2022.xlsx")

        conn_index = self.con_index
        cur = conn_index.cursor()
        table_index = """CREATE TABLE IF NOT EXISTS croisement_infogreffe_2018_2022(Siren VARCHAR,	
        Dénomination_2018 VARCHAR, Forme_Juridique_2018 VARCHAR, Code_APE_2018 VARCHAR,	Adresse_2018 VARCHAR,
        Code_postal_2018 VARCHAR, Num_dept_2018 VARCHAR, Code_Greffe_2018 VARCHAR, Date_immatriculation_2018 DATE,
        Date_radiation_2018 DATE, Statut_2018 VARCHAR,	Date_de_publication_2018 DATE, Millesime_1_2018 DATE,
        Date_de_cloture_exercice_1_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 INT,
        Millesime_2017 DATE, Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Résultat_2017 FLOAT, 
        Effectif_2017 INT, Millesime_2016 DATE,	Date_de_cloture_exercice_2016 DATE, CA_2016 FLOAT,
        Résultat_2016 FLOAT, Effectif_2016 INT, tranche_ca_millesime_2018 VARCHAR,
        tranche_ca_millesime_2017 VARCHAR, tranche_ca_millesime_2016 VARCHAR, 
        Dénomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,	Adresse_2022 VARCHAR,
        Code_postal_2022 VARCHAR, Num_dept_2022  VARCHAR, Code_Greffe_2022 VARCHAR,	Date_immatriculation_2022 DATE,
        Date_radiation_2022 DATE, Statut_2022 VARCHAR, Date_de_publication_2022 DATE, Millesime_2022 DATE,
        Date_de_cloture_exercice_2022 DATE, CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 INT,
        Millesime_2021 DATE, Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT,	Résultat_2021 FLOAT,
        Effectif_2021 INT,	Millesime_2020 DATE, Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT, Resultat_2020 FLOAT,
        Effectif_2020 INT, tranche_ca_millesime_2022 VARCHAR, tranche_ca_millesime_2021 VARCHAR,
        tranche_ca_millesime_2020 VARCHAR);
               """
        cur.execute(table_index)
        conn_index.commit()
        cur.close()

        col_to_keep = ["Siren", "Dénomination_2018", "Forme Juridique_2018", "Code APE_2018",	"Adresse_2018",
                     "Code postal_2018", "Num. dept._2018",	"Code Greffe_2018",	"Date immatriculation_2018",
                     "Date radiation_2018",	"Statut_2018", "Date de publication_2018", "Millesime 1_2018",
                     "Date de cloture exercice 1_2018",	"CA 1_2018", "Résultat 1_2018",
                     "Effectif 1_2018",	"Millesime 2_2018",	"Date de cloture exercice 2_2018", "Durée 2_2018",
                     "CA 2_2018", "Résultat 2_2018", "Effectif 2_2018", "Millesime 3_2018",
                     "Date de cloture exercice 3_2018", "CA 3_2018", "Résultat 3_2018",	"Effectif 3_2018",
                     "tranche_ca_millesime_1_2018",	"tranche_ca_millesime_2_2018",	"tranche_ca_millesime_3_2018",
                     "Dénomination_2022", "Forme Juridique_2022", "Code APE_2022", "Adresse_2022", "Code postal_2022",
                     "Num. dept._2022",	"Code Greffe_2022", "Date immatriculation_2022", "Date radiation_2022",
                     "Statut_2022",	"Geolocalisation_2022",	"Date de publication_2022",	"Millesime 1_2022",
                     "Date de cloture exercice 1_2022",	"CA 1_2022", "Résultat 1_2022",	"Effectif 1_2022",
                     "Millesime 2_2022", "Date de cloture exercice 2_2022",	"CA 2_2022", "Résultat 2_2022",
                     "Effectif 2_2022",	"Millesime 3_2022",	"Date de cloture exercice 3_2022", "CA 3_2022",
                     "Résultat 3_2022",	"Effectif 3_2022", "tranche_ca_millesime_1_2022", "tranche_ca_millesime_2_2022",
                     "tranche_ca_millesime_3_2022"]
        info_2018_2022[col_to_keep].to_sql('croisement_infogreffe_2018_2022', con=conn_index, if_exists='replace',
                                           index=False)

        info_2019_2022.to_excel(self.path_data_out + "croisement_infogreffe_2019_2022.xlsx")

        table_index = """CREATE TABLE IF NOT EXISTS croisement_infogreffe_2019_2022(Siren VARCHAR, 
        Dénomination_2019 VARCHAR, Forme_Juridique_2019 VARCHAR, Code_APE_2019 VARCHAR,	Adresse_2019 VARCHAR,
        Code_postal_2019 VARCHAR, Num_dept_2019 VARCHAR, Code_Greffe_2019 VARCHAR, Date_immatriculation_2019 DATE,
        Date_radiation_2019 DATE, Statut_2019 VARCHAR, Date_de_publication_2019 VARCHAR, Millesime_2019 DATE,
        Date_de_cloture_exercice_2019 DATE,	CA_2019 FLOAT, Résultat_2019 FLOAT,	Effectif_2019 INT,	
        Millesime_2018 DATE, Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT,	Résultat_2018 FLOAT, Effectif_2018 INT,
        Millesime_2017 DATE, Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Résultat_2017 FLOAT, Effectif_2017 INT,
        tranche_ca_millesime_2019 VARCHAR, tranche_ca_millesime_2018 VARCHAR, tranche_ca_millesime_2017,
        Dénomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,	Adresse_2022 VARCHAR,
        Code_postal_2022 VARCHAR, Num_dept_2022	VARCHAR, Code_Greffe_2022 VARCHAR, Date_immatriculation_2022 DATE,
        Date_radiation_2022 DATE, Statut_2022 VARCHAR, Date_de_publication_2022 DATE, Millesime_2022 DATE,
        Date_de_cloture_exercice_2022 DATE, CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 INT, Millesime_2021,
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT, Résultat_2021 FLOAT,	Effectif_2021 INT, Millesime_2020,
        Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT, Résultat_2020 FLOAT,	Effectif_2020 INT,
        tranche_ca_millesime_2022 VARCHAR, tranche_ca_millesime_2021 VARCHAR, tranche_ca_millesime_2020 VARCHAR);"""
        cur.execute(table_index)
        conn_index.commit()
        cur.close()
        info_2019_2022.to_sql('croisement_infogreffe_2019_2022', con=conn_index, if_exists='replace', index=False)

        info_2020_2022.to_excel(self.path_data_out + "croisement_infogreffe_2020_2022.xlsx")
        table_index = """CREATE TABLE IF NOT EXISTS croisement_infogreffe_2020_2022(Siren VARCHAR, 
        Dénomination_2020 VARCHAR, Forme_Juridique_2020 VARCHAR, Code_APE_2020 VARCHAR,	Adresse_2020 VARCHAR,
        Code_postal_2020 VARCHAR, Num_dept_2020 VARCHAR, Code_Greffe_2020 VARCHAR, Date_immatriculation_2020 DATE,
        Date_radiation_2020 DATE, Statut_2020 VARCHAR, Date_de_publication_2020 VARCHAR, Millesime_2020 DATE,
        Date_de_cloture_exercice_2020 DATE,	CA_2020 FLOAT, Résultat_2020 FLOAT,	Effectif_2020 INT,	
        Millesime_2019 DATE, Date_de_cloture_exercice_2019 DATE, CA_2019 FLOAT,	Résultat_2019 FLOAT, Effectif_2019 INT,
        Millesime_2018 DATE, Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 INT,
        tranche_ca_millesime_2020 VARCHAR, tranche_ca_millesime_2019 VARCHAR, tranche_ca_millesime_2017,
        Dénomination_2022 VARCHAR, Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,	Adresse_2022 VARCHAR,
        Code_postal_2022 VARCHAR, Num_dept_2022	VARCHAR, Code_Greffe_2022 VARCHAR, Date_immatriculation_2022 DATE,
        Date_radiation_2022 DATE, Statut_2022 VARCHAR, Date_de_publication_2022 DATE, Millesime_2022 DATE,
        Date_de_cloture_exercice_2022 DATE, CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 INT, Millesime_2021,
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT, Résultat_2021 FLOAT,	Effectif_2021 INT,
        tranche_ca_millesime_2022 VARCHAR, tranche_ca_millesime_2021 VARCHAR);"""
        cur.execute(table_index)
        conn_index.commit()
        cur.close()
        col_to_keep = ['Siren',	'Dénomination_2020', 'Forme Juridique_2020', 'Code APE_2020', 'Adresse_2020',
                       'Code postal_2020',	'Num. dept._2020', 'Code Greffe_2020', 'Date immatriculation_2020',
                       'Date radiation_2020', 'Statut_2020', 'Date de publication_2020', 'Millesime 1_2020',
                       'Date de cloture exercice 1_2020', 'Durée 1_2020', 'CA 1_2020',	'Résultat 1_2020',
                       'Effectif 1_2020', 'Millesime 2_2020', 'Date de cloture exercice 2_2020', 'Durée 2_2020',
                       'CA 2_2020', 'Résultat 2_2020', 'Effectif 2_2020', 'Millesime 3_2020',
                       'Date de cloture exercice 3_2020', 'CA 3_2020',	'Résultat 3_2020',	'Effectif 3_2020',
                       'tranche_ca_millesime_1_2020', 'tranche_ca_millesime_2_2020', 'tranche_ca_millesime_3_2020',
                       'Dénomination_2022', 'Forme Juridique_2022', 'Code APE_2022', 'Adresse_2022', 'Code postal_2022',
                       'Num. dept._2022', 'Code Greffe_2022', 'Date immatriculation_2022', 'Date radiation_2022',
                       'Statut_2022', 'Date de publication_2022	Millesime 1_2022', 'Date de cloture exercice 1_2022',
                       'CA 1_2022',	'Résultat 1_2022',	'Effectif 1_2022',	'Millesime 2_2022',
                       'Date de cloture exercice 2_2022	Durée 2_2022',	'CA 2_2022', 'Résultat 2_2022',
                       'Effectif 2_2022', 'tranche_ca_millesime_1_2022', 'tranche_ca_millesime_2_2022']
        info_2020_2022[col_to_keep].to_sql('croisement_infogreffe_2020_2022', con=conn_index, if_exists='replace',
                                           index=False)

        info_2021_2022.to_excel(self.path_data_out + "croisement_infogreffe_2021_2022.xlsx")
        table_index = """CREATE TABLE IF NOT EXISTS croisement_infogreffe_2020_2022(Siren VARCHAR,
        Dénomination_2021 VARCHAR, Forme_Juridique_2021 VARCHAR, Code_APE_2021 VARCHAR, Adresse_2021 VARCHAR,
                       Code_postal_2021 VARCHAR, Num_dept_2021 VARCHAR, Code_Greffe_2021 VARCHAR, 
                       Date_immatriculation_2021 DATE, Date_radiation_2021 DATE, Statut_2021 VARCHAR,
                        Date_de_publication_2021 DATE, Millesime_2021 DATE,
                       Date_de_cloture_exercice_2021 DATE, CA_2021 DATE, Résultat_2021 FLOAT, Effectif_2021 FLOAT, 
                       Millesime_2020 DATE, Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT, 
                       Résultat_2020 FLOAT, Effectif_2020 INT, Millesime_2019 DATE, Date_de_cloture_exercice_2019,
                       CA_2019 FLOAT, Résultat_2019 FLOAT, Effectif_2019 INT, tranche_ca_millesime_2021 VARCHAR,
                       tranche_ca_millesime_2020 VARCHAR, tranche_ca_millesime_2019 VARCHAR, Dénomination_2022 VARCHAR,
                       Forme_Juridique_2022 VARCHAR, Code_APE_2022 VARCHAR,	Adresse_2022 VARCHAR, 
                       Code_postal_2022 VARCHAR, Num_dept_2022 VARCHAR, Code_Greffe_2022 VARCHAR,
                        Date_immatriculation_2022 DATE, Date_radiation_2022 DATE, Statut_2022 VARCHAR,
                         Date_de_publication_2022 DATE, Millesime_2022 DATE, Date_de_cloture_exercice_2022 DATE,
                       CA_2022 FLOAT, Résultat_2022 FLOAT, Effectif_2022 INT, tranche_ca_millesime_2022 VARCHAR);"""
        cur.execute(table_index)
        conn_index.commit()
        cur.close()

        col_to_keep = ['Siren',	'Dénomination_2021', 'Forme Juridique_2021', 'Code APE_2021', 'Adresse_2021',
                       'Code postal_2021', 'Num. dept._2021', 'Code Greffe_2021', 'Date immatriculation_2021',
                       'Date radiation_2021', 'Statut_2021', 'Date de publication_2021', 'Millesime 1_2021',
                       'Date de cloture exercice 1_2021', 'CA 1_2021', 'Résultat 1_2021', 'Effectif 1_2021',
                       'Millesime 2_2021', 'Date de cloture exercice 2_2021', 'Durée 2_2021', 'CA 2_2021',
                       'Résultat 2_2021', 'Effectif 2_2021', 'Millesime 3_2021', 'Date de cloture exercice 3_2021',
                       'CA 3_2021', 'Résultat 3_2021', 'Effectif 3_2021', 'tranche_ca_millesime_1_2021',
                       'tranche_ca_millesime_2_2021', 'tranche_ca_millesime_3_2021', 'Dénomination_2022',
                       'Forme Juridique_2022', 'Code APE_2022',	'Adresse_2022',	'Code postal_2022',
                       'Num. dept._2022', 'Code Greffe_2022', 'Date immatriculation_2022', 'Date radiation_2022',
                       'Statut_2022', 'Date de publication_2022', 'Millesime 1_2022', 'Date de cloture exercice 1_2022',
                       'CA 1_2022', 'Résultat 1_2022', 'Effectif 1_2022', 'tranche_ca_millesime_1_2022']
        info_2021_2022[col_to_keep].to_sql('croisement_infogreffe_2021_2022', con=conn_index, if_exists='replace',
                                           index=False)

    def get_etired_year(self, liste_data):
        data_etired = liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2021.csv"],
                                                                lsuffix="_2018", rsuffix="_2021", how="inner")
        data_etired.to_excel(self.path_data_out + "croisement_infogreffe_2018_2021.xlsx")

        conn_index = self.con_index
        cur = conn_index.cursor()
        table_index = """CREATE TABLE IF NOT EXISTS croisement_infogreffe_2018_2022(Siren VARCHAR,	
        Dénomination_2018 VARCHAR, Forme_Juridique_2018 VARCHAR, Code_APE_2018 VARCHAR,	Adresse_2018 VARCHAR,
        Code_postal_2018 VARCHAR, Num_dept_2018 VARCHAR, Code_Greffe_2018 VARCHAR, Date_immatriculation_2018 DATE,
        Date_radiation_2018 DATE, Statut_2018 VARCHAR,	Date_de_publication_2018 DATE, Millesime_2018 DATE,
        Date_de_cloture_exercice_2018 DATE, CA_2018 FLOAT, Résultat_2018 FLOAT, Effectif_2018 INT,
        Millesime_2017 DATE, Date_de_cloture_exercice_2017 DATE, CA_2017 FLOAT, Résultat_2017 FLOAT, 
        Effectif_2017 INT, Millesime_2016 DATE,	Date_de_cloture_exercice_2016 DATE, CA_2016 FLOAT,
        Résultat_2016 FLOAT, Effectif_2016 INT, tranche_ca_millesime_2018 VARCHAR,
        tranche_ca_millesime_2017 VARCHAR, tranche_ca_millesime_2016 VARCHAR, 
        Dénomination_2021 VARCHAR, Forme_Juridique_2021 VARCHAR, Code_APE_2021 VARCHAR,	Adresse_2021 VARCHAR,
        Code_postal_2021 VARCHAR, Num_dept_2021  VARCHAR, Code_Greffe_2021 VARCHAR,	Date_immatriculation_2021 DATE,
        Date_radiation_2021 DATE, Statut_2021 VARCHAR, Date_de_publication_2021 DATE, Millesime_2021 DATE,
        Date_de_cloture_exercice_2021 DATE, CA_2021 FLOAT, Résultat_2021 FLOAT, Effectif_2021 INT,
        Millesime_2020 DATE, Date_de_cloture_exercice_2020 DATE, CA_2020 FLOAT,	Résultat_2020 FLOAT,
        Effectif_2020 INT,	Millesime_2019 DATE, Date_de_cloture_exercice_2019 DATE, CA_2019 FLOAT, Resultat_2019 FLOAT,
        Effectif_2019 INT, tranche_ca_millesime_2021 VARCHAR, tranche_ca_millesime_2020 VARCHAR,
        tranche_ca_millesime_2019 VARCHAR);
               """
        cur.execute(table_index)
        conn_index.commit()
        cur.close()

        col_to_keep = ["Siren", "Dénomination_2018", "Forme Juridique_2018", "Code APE_2018",	"Adresse_2018",
                     "Code postal_2018", "Num. dept._2018",	"Code Greffe_2018",	"Date immatriculation_2018",
                     "Date radiation_2018",	"Statut_2018", "Date de publication_2018", "Millesime 1_2018",
                     "Date de cloture exercice 1_2018",	"CA 1_2018", "Résultat 1_2018",
                     "Effectif 1_2018",	"Millesime 2_2018",	"Date de cloture exercice 2_2018", "Durée 2_2018",
                     "CA 2_2018", "Résultat 2_2018", "Effectif 2_2018", "Millesime 3_2018",
                     "Date de cloture exercice 3_2018", "CA 3_2018", "Résultat 3_2018",	"Effectif 3_2018",
                     "tranche_ca_millesime_1_2018",	"tranche_ca_millesime_2_2018",	"tranche_ca_millesime_3_2018",
                     "Dénomination_2022", "Forme Juridique_2022", "Code APE_2022", "Adresse_2022", "Code postal_2022",
                     "Num. dept._2022",	"Code Greffe_2022", "Date immatriculation_2022", "Date radiation_2022",
                     "Statut_2022",	"Geolocalisation_2022",	"Date de publication_2022",	"Millesime 1_2022",
                     "Date de cloture exercice 1_2022",	"CA 1_2022", "Résultat 1_2022",	"Effectif 1_2022",
                     "Millesime 2_2022", "Date de cloture exercice 2_2022",	"CA 2_2022", "Résultat 2_2022",
                     "Effectif 2_2022",	"Millesime 3_2022",	"Date de cloture exercice 3_2022", "CA 3_2022",
                     "Résultat 3_2022",	"Effectif 3_2022", "tranche_ca_millesime_1_2022", "tranche_ca_millesime_2_2022",
                     "tranche_ca_millesime_3_2022"]
        data_etired[col_to_keep].to_sql('croisement_infogreffe_2018_2021', con=conn_index, if_exists='replace',
                                           index=False)


        return data_etired

    def preprocess_data(self, data):
        data.drop(columns=["id","Nic_2019","Nic_2020","Nic_2021"], inplace=True)
        return data


    def all_companies(self, liste_data):
        i=0
        for file in liste_data.values():
            if i >0:
                try :
                    file.set_index("Siren", inplace=True)
                    file.drop(columns=["Nic", "Greffe", "Région", "Département", "Greffe", "fiche_identite", "Ville",
                                       "Département", "Région"], inplace=True)

                except KeyError: pass
            else: print("i=0")

            i+=1
        data_final = liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2019.csv"],
                                                              lsuffix="_2018", rsuffix="_2019", how="outer")
        data_final = data_final.join(liste_data["chiffres-cles-2020.csv"], rsuffix="_2020", how="outer")
        data_final = data_final.join(liste_data["chiffres-cles-2021.csv"], rsuffix="_2021", how="outer")
        data_final = data_final.join(liste_data["chiffres-cles-2022.csv"], rsuffix="_2022", how="outer")
        return data_final



    def get_turnover_data(self, data):
        all_turnover = pd.DataFrame([data.index,
                                     data["CA 3_2018"], data["CA 2_2018"], data["CA 1_2018"],
                                     data["CA 3_2021"], data["CA 2_2021"], data["CA 1_2021"],
                                     data["CA 1_2022"]]).transpose()
        all_turnover.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019",
                                     5: "2020", 6: "2021", 7: "2022"}, inplace=True)
        all_turnover.dropna(inplace=True)
        all_turnover.to_csv(self.path_data_out + "turnover_french_companies_2016_2021.csv", sep=",", index=False)

        conn_index = self.con_index
        cur = conn_index.cursor()
        table_index = """CREATE TABLE IF NOT EXISTS turnover_french_companies(siren VARCHAR, turnover_2016 FLOAT, 
        turnover_2017 FLOAT, turnover_2018 FLOAT, turnover_2019 FLOAT, turnover_2020 FLOAT, turnover_2021 FLOAT,
        turnover_2022 FLOAT);
               INSERT INTO turnover_french_companies(siren, turnover_2016, turnover_2017, turnover_2018, turnover_2019,
                turnover_2020, turnover_2021, turnover_2022)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
               """
        cur.execute(table_index, (data['Siren'], data['2016'], data['2017'], data['2018'], data['2019'],
                                  data['2020'], data['2021'], data['2022']))
        conn_index.commit()
        cur.close()


    def get_employees_data(self, data):
        all_employees = pd.DataFrame([data.index,
                                      data["Effectif 3_2018"], data["Effectif 2_2018"], data["Effectif 1_2018"],
                                      data["Effectif 3_2021"], data["Effectif 2_2021"], data["Effectif 1_2021",
                                      data["Effectif 1_2022"]]]
                                     ).transpose()
        all_employees.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019", 5: "2020", 6: "2021",
                                      7: "2022"},
                             inplace=True)
        all_employees.dropna(inplace=True)
        all_employees.to_csv(self.path_data_out + "workforce_french_companies_2016_2022.csv", sep=",", index=False)

        conn_index = self.con_index
        cur = conn_index.cursor()
        table_index = """CREATE TABLE IF NOT EXISTS turnover_french_companies(siren VARCHAR, workforce_2016 FLOAT, 
        workforce_2017 FLOAT, workforce_2018 FLOAT, workforce_2019 FLOAT, workforce_2020 FLOAT, workforce_2021 FLOAT,
        workforce_2022 FLOAT);
               INSERT INTO turnover_french_companies(siren, workforce_2016, workforce_2017, workforce_2018, 
               workforce_2019, workforce_2020, workforce_2021, workforce_2022)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
               """
        cur.execute(table_index, (data['Siren'], data['2016'], data['2017'], data['2018'], data['2019'],
                                  data['2020'], data['2021'], data['2022']))
        conn_index.commit()
        cur.close()

    def get_results_data(self, data):
            all_results = pd.DataFrame([data.index,
                                        data["Résultat 3_2018"], data["Résultat 2_2018"], data["Résultat 1_2018"],
                                        data["Résultat 3_2021"], data["Résultat 2_2021"], data["Résultat 1_2021"],
                                        data["Résultat 1_2022"]]
                                       ).transpose()
            all_results.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019", 5: "2020",
                                        6: "2021", 7: "2022"}, inplace=True)
            all_results.dropna(inplace=True)
            all_results.to_csv(self.path_data_out + "earning_french_companies_2016_2022.csv", sep=",", index=False)

            conn_index = self.con_index
            cur = conn_index.cursor()
            table_index = """CREATE TABLE IF NOT EXISTS turnover_french_companies(siren VARCHAR, earning_2016 FLOAT, 
            earning_2017 FLOAT, earning_2018 FLOAT, earning_2019 FLOAT, earning_2020 FLOAT, earning_2021 FLOAT,
            earning_2022 FLOAT);
                   INSERT INTO turnover_french_companies(siren, earning_2016, earning_2017, earning_2018, earning_2019,
                   earning_2020, earning_2021, earning_2022)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                   """
            cur.execute(table_index, (data['Siren'], data['2016'], data['2017'], data['2018'], data['2019'],
                                      data['2020'], data['2021'], data['2022']))
            conn_index.commit()
            cur.close()
            return all_results
