#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 14:37:58 2022

@author: nsauzeat
"""
import pandas as pd
import os


class Infogreffe():
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = f"{self.path}/data/data_in"
        self.path_data_infogreffe = f"{self.path_data_in}/data_infogreffe"
        self.path_data_out = f"{self.path}/data/data_out"

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
                                       "Département", "Région"], inplace=True)
                except KeyError: pass
            i+=1
        data_final= liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2019.csv"],
                                                              lsuffix="_2018", rsuffix="_2019", how="inner")
        data_final= data_final.join(liste_data["chiffres-cles-2020.csv"].set_index("Siren"),
                                    rsuffix="_2020", how="inner")
        data_final= data_final.join(liste_data["chiffres-cles-2021.csv"], rsuffix="_2021", how="inner")
        data_final =data_final.join(liste_data["chiffres-cles-2022.csv"], rsuffix="_2022", how="inner")
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
        info_2019_2022.to_excel(self.path_data_out + "croisement_infogreffe_2019_2022.xlsx")
        info_2020_2022.to_excel(self.path_data_out + "croisement_infogreffe_2020_2022.xlsx")
        info_2021_2022.to_excel(self.path_data_out + "croisement_infogreffe_2021_2022.xlsx")

    def get_etired_year(self, liste_data):
        data_etired = liste_data["chiffres-cles-2018.csv"].join(liste_data["chiffres-cles-2021.csv"],
                                                                lsuffix="_2018", rsuffix="_2021", how="inner")
        data_etired.to_excel(self.path_data_out + "croisement_infogreffe_2018_2021.xlsx")
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
                                     data["CA 3_2021"], data["CA 2_2021"], data["CA 1_2021"]]).transpose()
        all_turnover.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019",
                                     5: "2020", 6: "2021"}, inplace=True)
        all_turnover.dropna(inplace=True)
        all_turnover.to_csv(self.path_data_out + "turnover_french_companies_2016_2021.csv", sep=",", index=False)

    def get_employees_data(self, data):
        all_employees = pd.DataFrame([data.index,
                                      data["Effectif 3_2018"], data["Effectif 2_2018"], data["Effectif 1_2018"],
                                      data["Effectif 3_2021"], data["Effectif 2_2021"], data["Effectif 1_2021"]]
                                     ).transpose()
        all_employees.rename(columns={0:"Siren",1:"2016",2:"2017",3:"2018",4:"2019",5:"2020",6:"2021"}, inplace=True)
        all_employees.dropna(inplace=True)
        all_employees.to_csv(self.path_data_out + "workforce_french_companies_2016_2021.csv", sep=",", index=False)

    def get_results_data(self, data):
            all_results = pd.DataFrame([data.index,
                                        data["Résultat 3_2018"], data["Résultat 2_2018"], data["Résultat 1_2018"],
                                        data["Résultat 3_2021"], data["Résultat 2_2021"], data["Résultat 1_2021"]]
                                       ).transpose()
            all_results.rename(columns={0: "Siren", 1: "2016", 2: "2017", 3: "2018", 4: "2019", 5: "2020",
                                        6: "2021"}, inplace=True)
            all_results.dropna(inplace=True)
            all_results.to_csv(self.path_data_out + "earning_french_companies_2016_2021.csv", sep=",", index=False)
            return all_results
