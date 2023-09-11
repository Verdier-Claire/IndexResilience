#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 14:37:58 2022

@author: nsauzeat
"""
import pandas as pd
import os
import json
from preprocess.request_sql import RequestSql

class Infogreffe():
    def __init__(self):
        self.path = 'C:\\Users\\OpenStudio.Aurora-R13\\Desktop\\IndexResilience'
        self.path_data_in = f"{self.path}/data/data_in"
        self.path_data_infogreffe = f"{self.path_data_in}/data_infogreffe"
        self.path_data_out = f"{self.path}/data/data_out"


    def read_excel(self):
        list_file = {}
        for filename in os.listdir(self.path_data_infogreffe):
            if filename.endswith(".csv"):
                print(filename)
                list_file[filename] = pd.read_csv(self.path_data_infogreffe+"/"+filename, sep=";", header=0,
                                                        parse_dates=['Date immatriculation', 'Date radiation',
                                                                     'Date de publication',
                                                                     'Date de cloture exercice 1',
                                                                     'Date de cloture exercice 2',
                                                                     'Date de cloture exercice 3'],
                                                        dtype={'Dénomination': str, 'Siren': str, 'Nic': str,
                                                               'Forme Juridique': str, 'Code APE': str,
                                                               'Adresse': str, 'Code postal': str,
                                                               'Ville': str, 'Num. dept.': str, 'Département': str,
                                                               'Région': str, 'Code Greffe': str, 'Statut': str,
                                                               'CA 1': float, 'Résultat 1': float, 'Effectif 1': float,
                                                               'Durée 2': str, 'CA 2': float, 'Résultat 2': float,
                                                               'Effectif 2': float,
                                                               'CA 3': float, 'Résultat 3': float, 'Effectif 3': float,
                                                               'tranche_ca_millesime_1': str,
                                                               'tranche_ca_millesime_2': str,
                                                               'tranche_ca_millesime_3': str, 'Millesime 1': str,
                                                                'Millesime 2': str, 'Millesime 3': str})
                list_file[filename] = list_file[filename].replace({pd.NaT: None})

        return list_file


    def sorted_companies(self, liste_data):
        for file in liste_data.values():
            try:
                file["Siren"] = file["Siren"].astype(str)
                file.set_index("Siren", inplace=True)
            except KeyError:
                    file.reset_index(inplace=True)
                    try:
                        file["Siren"].astype(str)
                        file["Siren"] =file["Siren"].str.split(".")[0]
                        file.set_index("Siren", inplace=True)
                    except KeyError:
                        pass
            try:
                file.drop(columns=["Nic", "Greffe", "Région", "Département", "Greffe", "fiche_identite", "Ville",
                                   "Geolocalisation", "Durée 2", "Durée 1", "Durée 3",
                                   "Libellé APE", 'id'],
                          inplace=True)
            except KeyError: pass

        data_final= liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2019.csv"],
                                                              lsuffix="_2018", rsuffix="_2019",
                                                              how="inner", on='Siren')

        data_final= data_final.join(liste_data["chiffres-cles-2020.csv"],
                                    rsuffix="_2020", how="inner", on='Siren')
        data_final= data_final.join(liste_data["chiffres-cles-2021.csv"], rsuffix="_2021",
                                    how="inner", on='Siren')
        data_final =data_final.join(liste_data["chiffres-cles-2022.csv"], rsuffix="_2022",
                                    how="inner", on='Siren')
        return data_final, liste_data

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
        # create table from 2018 to 2022
        self.get_2018_2022(liste_data)
        # create table from 2019 to 2022
        self.get_2019_2022(liste_data)
        # create table from 2020 to 2022
        self.get_2020_2022(liste_data)
        # create table from 2021 to 2022
        self.get_2021_2022(liste_data)


    def get_etired_year(self, liste_data):
        data_etired = liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2021.csv"],
                                                                lsuffix="_2018", rsuffix="_2021", how="inner")
        data_etired = data_etired.join(liste_data["chiffres-cles-2022.csv"], how="inner")
        data_etired.reset_index(names='Siren', inplace=True)

        name_table = 'croisement_infogreffe_2018_2021'
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]

        # create table croisement_infogreffe_2018_2021
        RequestSql().create_table_croisement_infogreffe_2018_2021()
        RequestSql().execute_values(data_etired[col_to_keep], name_table)
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
                                     data["CA 1"]]).transpose()
        all_turnover.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019",
                                     5: "2020", 6: "2021", 7: "2022"}, inplace=True)
        all_turnover.dropna(inplace=True)

        RequestSql().create_table_turnover_french_companies()
        RequestSql().execute_values(all_turnover, "turnover_french_companies")



    def get_employees_data(self, data):
        all_employees = pd.DataFrame([data.index,
                                      data["Effectif 3_2018"], data["Effectif 2_2018"], data["Effectif 1_2018"],
                                      data["Effectif 3_2021"], data["Effectif 2_2021"], data["Effectif 1_2021"],
                                      data["Effectif 1"]]).transpose()
        all_employees.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019", 5: "2020", 6: "2021",
                                      7: "2022"},
                             inplace=True)
        all_employees.dropna(inplace=True)
        all_employees.to_csv(self.path_data_out + "workforce_french_companies_2016_2022.csv", sep=",", index=False)

        RequestSql().create_table_employees_french_companies()
        RequestSql().execute_values(all_employees, "employees_french_companies")


    def get_results_data(self, data):
            all_results = pd.DataFrame([data.index,
                                        data["Résultat 3_2018"], data["Résultat 2_2018"], data["Résultat 1_2018"],
                                        data["Résultat 3_2021"], data["Résultat 2_2021"], data["Résultat 1_2021"],
                                        data["Résultat 1"]]
                                       ).transpose()
            all_results.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019", 5: "2020",
                                        6: "2021", 7: "2022"}, inplace=True)
            all_results.dropna(inplace=True)
            all_results.to_csv(self.path_data_out + "earning_french_companies_2016_2022.csv", sep=",", index=False)

            RequestSql().create_table_results_french_companies()
            RequestSql().execute_values(all_results, "results_french_companies")
            return all_results

    def get_2018_2019(self, infogreffe_first_year_comp):

        infogreffe_first_year_comp.rename(columns={"Siren_x": 'Siren'}, inplace=True)
        name_table = 'croisement_infogreffe_2018_2019'
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]

        RequestSql().create_table_croisement_infogreffe_2018_2019()
        RequestSql().execute_values(infogreffe_first_year_comp[col_to_keep],
                                    name_table)

    def get_2018_2022(self, liste_data):
        info_2018_2022 = liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2018", rsuffix="_2022", how="inner")

        info_2018_2022.reset_index(names='Siren', inplace=True)

        # create table croisement_infogreffe_2018_2022
        RequestSql().create_table_croisement_infogreffe_2018_2022()

        name_table = 'croisement_infogreffe_2018_2022'
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]
        RequestSql().execute_values(info_2018_2022[col_to_keep], name_table)

    def get_2019_2022(self, liste_data):
        info_2019_2022 = liste_data["chiffres-cles-2019.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2019", rsuffix="_2022", how="inner")

        info_2019_2022.reset_index(names='Siren', inplace=True)

        # create table croisement_infogreffe_2019_2022
        name_table = 'croisement_infogreffe_2019_2022'
        RequestSql().create_table_croisement_infogreffe_2019_2022()
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]
        RequestSql().execute_values(info_2019_2022[col_to_keep], name_table)

    def get_2020_2022(self, liste_data):
        info_2020_2022 = liste_data["chiffres-cles-2020.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2020", rsuffix="_2022", how="inner")

        info_2020_2022.reset_index(names='Siren', inplace=True)

        # create table croisement_infogreffe_2020_2022
        RequestSql().create_table_croisement_infogreffe_2020_2022()
        name_table = 'croisement_infogreffe_2020_2022'
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]

        RequestSql().execute_values(info_2020_2022[col_to_keep], name_table)

    def get_2021_2022(self, liste_data):
        info_2021_2022 = liste_data["chiffres-cles-2021.csv"].join(liste_data["chiffres-cles-2022.csv"],
                                                                   lsuffix="_2021", rsuffix="_2022", how="inner")
        # create table croisement_infogreffe_2021_2022
        info_2021_2022.reset_index(names='Siren', inplace=True)
        name_table = 'croisement_infogreffe_2021_2022'
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]

        RequestSql().create_table_croisement_infogreffe_2021_2022()
        RequestSql().execute_values(info_2021_2022[col_to_keep], name_table)
