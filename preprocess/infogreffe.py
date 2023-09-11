#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 11:33:10 2022

@author: nsauzeat
"""
import os

import pandas as pd
import math
import psycopg2
import ipdb
from select_data import Infogreffe
import psycopg2.extras as extras
from preprocess.request_sql import RequestSql
import json


class Preprocess():
    def __init__(self):
        self.path = 'C:\\Users\\OpenStudio.Aurora-R13\\Desktop\\IndexResilience'
        self.path_data_in = f"{self.path}/data/data_in"
        self.path_data_infogreffe = f"{self.path_data_in}/data_infogreffe"
        self.path_data_out = f"{self.path}/data/data_out/"
        self.conn_pappers = psycopg2.connect(
           user="iat",
           password="bR3fTAk2VkCNbDPg",
           host="localhost",
           port="5432",
           database="pappers"
              )
        self.cursor = self.conn_pappers.cursor()

    def recup_siret_list(self):
            """
            Parameters
            ----------
            sprite : TYPE
                DESCRIPTION.
    
            Returns
            -------
            siret_list : TYPE
                DESCRIPTION.
            """
            conn = psycopg2.connect(
           user = "iat",
           password = "bR3fTAk2VkCNbDPg",
           host = "localhost",
           port = "5432",
           database = "iat"
              )
            cur = conn.cursor()
            select_list_siret_establishment =  """SELECT e.siret 
                                        FROM establishment e"""
            cur.execute(select_list_siret_establishment)
            siret_list = cur.fetchall()
            conn.commit()
            siret_list =[item for t in siret_list for item in t]
            return siret_list, conn

    

    def read_data(self):
        data = pd.read_csv(self.path_data_in+"/all_establishments_france.csv", sep=";", header=0,
                           dtype={'siret': str, 'siren': str, 'nic': str})
        data.rename(columns={"activitePrincipaleEtablissement": 'NAF',
                             'nomenclatureActivitePrincipaleEtablissement': 'ACTIVITE',
                             'siren': 'Siren'}, inplace=True)
        return data

    def select_atlas(self, data,liste_siret):
        data["siret"] = data["siret"].str.replace("-", "")
        data_final = data[data["siret"].isin(liste_siret)]
        return data_final

    def select_activity(self, conn):
        cur = conn.cursor()
        select_list_activity = """ SELECT DISTINCT meta -> 'activity'
                                    FROM establishment 
                                    WHERE meta -> 'activity' IS NOT NULL
                                    AND meta -> 'sector' IS NOT NULL
                               """
        cur.execute(select_list_activity)
        list_activity = cur.fetchall()
        conn.commit()
        list_activity =[item for t in list_activity for item in t]
        return list_activity


    def select_company(self, data, list_activity):
        # data["NAF"] = data["NAF"].astype(str)
        # list_activity = [item.split(".")[0] + item.split(".")[1] for item in list_activity]
        data_final = data[data["NAF"].isin(list_activity)]
        return data_final

    def get_clean_data(self, data):
        data.drop(columns=["NAF","ACTIVITE"], inplace=True)
        return data

    def get_all_data(self, data_final, infogreffe):
        name_table = 'croisement_infogreffe_complet_2022'
        infogreffe = self.pretraitement_data(infogreffe)
        data_final = self.pretraitement(data_final)

        all_info= pd.merge(infogreffe, data_final, on='Siren', how="inner")
        incomplete_data = data_final[~data_final.index.isin(all_info.index.tolist())]
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]

        all_info = all_info.replace({pd.NaT: None})

        RequestSql().create_table_croisement_infogreffe_2018_2022()
        RequestSql().execute_values(all_info[col_to_keep], name_table)

        return all_info, incomplete_data

    def pretraitement_data(self, data):
        data.reset_index(inplace=True)
        data["Siren"] = data["Siren"].astype(str)
        data = data.loc[data["Siren"]!="nan"]
        data["Siren"].apply(lambda x: x.split(".")[0])
        data["Siren"] = data["Siren"].astype(str)
        data["Siren"] = data["Siren"].apply(lambda x: x.split(".")[0])
        data["Siren"] = data["Siren"].apply(lambda x : "0" + x if len(x)<9 else x)
        return data

    def pretraitement(self, data_final):
        data_final["Siren"] = data_final["Siren"].astype(str)
        data_final["Siren"] = data_final["Siren"].apply(lambda x: "0" + x if len(x)<9 else x)
        return data_final

    def get_incomplete_year(self, data, infogreffe):
        data = Preprocess().pretraitement(data)
        infogreffe = Preprocess().pretraitement_data(infogreffe)
        info_first_year = pd.merge(infogreffe, data, left_on=infogreffe["Siren"] , right_on=data["Siren"] ,how="inner" )
        incomplete_data = data[~data.index.isin(info_first_year.index.tolist())]

        return info_first_year, incomplete_data


    def manipulate_localisation(self, data):
        data["Changement Adresse"] = ""

        #data["Changement Adresse"] = data.apply(lambda x : True if x[])

    def arrange_ca(self, data):
        # data = data.apply(lambda x: Preprocess().get_ca(x),axis=1)
        name_table = 'croisement_infogreffe_2019_2020'
        data.reset_index(names='Siren', inplace=True)
        with open(f"{self.path}/data/col_to_keep.json", 'r', encoding="utf-8") as f:
            col_to_keep = json.load(f)[name_table]

        RequestSql().create_table_croisement_infogreffe_2019_2020()
        RequestSql().execute_values(data[col_to_keep], name_table)

    def get_ca(self, data):
        if  math.isnan(data["CA 1_2019"]) and math.isnan(data["CA 2_2019"]):
                 data["CA 1_2019"] = data["CA 2_2020"]
                 if  math.isnan(data["CA 1_2019"]):
                     data["CA 1_2019"] = data["CHIFFRE_D'AFFAIRES"]
                     data["CA 2_2019"] = data["CHIFFRE_D'AFFAIRES"]
                 data["CA 2_2019"] =data["CA 3_2020"]
        elif data["CA 2_2019"]=="None":
            data["CA 2_2019"]= data["CA 3_2020"]
        return data

    def take_naf_cgt(self, data):
        data["Changement NAF"]=  False
        data = data.apply(lambda x: self.get_cgt_naf(x),axis=1)
        return data

    def get_cgt_naf(self, x):
        try:

            if (x["Code APE_2018"]!= x["Code APE_2019"] and pd.isna(x["Code APE_2021"])!=True and pd.isna(x["Code APE_2018"])!=True ) | \
                (x["Code APE_2018"]!= x["Code APE"] and pd.isna(x["Code APE"])!=True  and pd.isna(x["Code APE_2018"])!=True) | \
                 (x["Code APE_2018"]!= x["Code APE_2021"] and pd.isna(x["Code APE_2021"])!=True  and pd.isna(x["Code APE_2018"])!=True):
    
                    x["Changement NAF"]= True

            elif (pd.isna(x["Code APE_2018"])==True) and (pd.isna(x["Code APE_2019"])!=True):
                 if (x["Code APE_2019"]!= x["Code APE"] and pd.isna(x["Code APE"])!=True  and pd.isna(x["Code APE_2019"])!=True) | \
                 (x["Code APE_2019"]!= x["Code APE_2021"] and pd.isna(x["Code APE_2021"])!=True  and pd.isna(x["Code APE_2019"])!=True):
    
                    x["Changement NAF"]= True
            elif (pd.isna(x["Code APE_2018"])==True) and (pd.isna(x["Code APE_2019"])==True) and (pd.isna(x["Code APE"])!=True):
                if (x["Code APE_2019"]!= x["Code APE_2021"] and pd.isna(x["Code APE_2021"])!=True  and pd.isna(x["Code APE_2019"])!=True):
                    x["Changement NAF"]= True
            else: x["Changement NAF"]= False
        except TypeError:
            ipdb.set_trace()
        return x

    def take_only_cgt(self, data):
        cgt_naf = data.loc[data["Changement NAF"]==True]
        cgt_naf = cgt_naf[~cgt_naf.index.duplicated(keep="first")]
        liste_cgt = cgt_naf[["Code APE_2018","Code APE","Code APE_2019","Code APE_2021"]]
        liste_cgt_clean = pd.DataFrame(columns=["source", "target"], index= cgt_naf.index)
        liste_cgt_clean["source"] = liste_cgt.apply(lambda x: Preprocess.put_in_form(x, liste_cgt_clean),axis=1)
        liste_cgt_clean["target"] = liste_cgt.apply(lambda x: Preprocess.second_put_form(x, liste_cgt_clean),axis=1)
        return cgt_naf, liste_cgt_clean


    def put_in_form(self, x,liste_cgt_clean):
            if  pd.isna(x["Code APE_2018"]) != True:
                   liste_cgt_clean.loc[x.name,"source"] = x["Code APE_2018"]
            elif pd.isna(x["Code APE_2019"]) != True and  pd.isna(x["Code APE_2018"]) == True:
                   liste_cgt_clean.loc[x.name,"source"] = x["Code APE_2019"]
            elif pd.isna(x["Code APE"]) != True and  pd.isna(x["Code APE_2018"]) == True and  pd.isna(x["Code APE_2019"]) == True:
                   liste_cgt_clean.loc[x.name,"source"] = x["Code APE"]
            else: ipdb.set_trace()
            return liste_cgt_clean.loc[x.name, "source"]


    def second_put_form(self, x,liste_cgt_clean):
            if  (pd.isna(x["Code APE_2019"]) != True) and (x["Code APE_2019"] != x["Code APE_2018"]) :
                liste_cgt_clean.loc[x.name,"target"] = x["Code APE_2019"]
            else:
                if (pd.isna(x["Code APE"]) != True) and (x["Code APE"]!=x["Code APE_2018"]) :
                    liste_cgt_clean.loc[x.name,"target"] = x["Code APE"]
                else:
                    if(pd.isna(x["Code APE_2021"]) != True)  and (x["Code APE_2021"]!=x["Code APE_2018"]) :
                        liste_cgt_clean.loc[x.name,"target"] = x["Code APE_2021"]
            return liste_cgt_clean.loc[x.name, "target"]

    def count_occur(self, data):
        grouped_count_2= data.groupby(["source","target"]).size()
        grouped_count_2= pd.DataFrame(grouped_count_2.reset_index())
        grouped_count_2 = grouped_count_2[(grouped_count_2["target"]!="0000Z") & (grouped_count_2["target"]!="000Z") ]
        grouped_count_2.rename(columns={0:"count"}, inplace=True)
        grouped_count = grouped_count_2[pd.isna(grouped_count_2["target"])!=True]
        grouped_count.to_csv(self.path_data_out + "/changement_naf.csv", sep=";", index=False)


    def preprocess_data(self, liste_data):
        for file in liste_data.values():
            try:
                file["Siren"]=  file["Siren"].astype(str)
                file.set_index("Siren", inplace=True)
            except KeyError:
                try:
                    file.reset_index(inplace=True)
                    file["Siren"]= file["Siren"].apply(lambda x: x.split(".")[0])
                    file.set_index("Siren", inplace=True)
                except ValueError:
                    try:
                        file.drop(columns="level_0", inplace=True)
                        file.reset_index(inplace=True)
                        file["Siren"]=  file["Siren"].apply(lambda x: x.split(".")[0])
                        file.set_index("Siren", inplace=True)
                    except KeyError: pass

                except KeyError: pass
        return liste_data

def main():

    liste_data = Infogreffe().read_excel()
    data_infogreffe, liste_data = Infogreffe().sorted_companies(liste_data)
    # data_final_clean = Preprocess().get_clean_data(data_final)
    # liste_data = Preprocess().preprocess_data(liste_data)

    # Function that stored all establishment in Atlas Tables
    liste_siret, conn = Preprocess().recup_siret_list()
    # Function that read Arnault database
    data_atlas = Preprocess().read_data()
    # Function that select only establishment in Atlas Table
    # data_atlas = Preprocess().select_atlas(data, liste_siret)
    # Function that select only activities inside Atlas app
    list_activity = Preprocess().select_activity(conn)
    # Function that select in Arnault's dataset only establishment with  included Atlas's activities
    data_final = Preprocess().select_company(data_atlas, list_activity)

    establishment_final, incomplete_data = Preprocess().get_all_data(data_final,data_infogreffe)

    print("Fin du pré-processing")
    infogreffe_only_2first,infogreffe_first_year_comp = Infogreffe().get_first_year_comp(liste_data)
    infogreffe_first_year_comp, incomplete_data = Preprocess().get_incomplete_year(incomplete_data,infogreffe_first_year_comp)
    # infogreffe_first_year_comp.to_excel(f"{os.getcwd()}/data/data_out/croisement_infogreffe_2018_2019.xlsx")
    #Infogreffe().get_2018_2019(infogreffe_first_year_comp)
    info_only_2midlle, infogreffe_middle_year_comp = Infogreffe().get_midlle_year_comp(liste_data)
    infogreffe_middle_year_comp, incomplete_data = Preprocess().get_incomplete_year(incomplete_data, infogreffe_middle_year_comp)

    infogreffe_etired_year_comp = Infogreffe().get_etired_year(liste_data)

    #infogreffe_etired_year_comp, incomplete_data  = Preprocess.get_all_data(incomplete_data, infogreffe_etired_year_comp)


    Preprocess().arrange_ca(infogreffe_middle_year_comp)
    #infogreffe_etired_year_comp= Preprocess.manipulate_localisation(infogreffe_etired_year_comp)
    # all_data = Infogreffe().all_companies(liste_data)

    # data_changement_naf = Preprocess().take_naf_cgt(all_data)

    # naf_changement,list_cgt = Preprocess().take_only_cgt(data_changement_naf)
    # Preprocess().count_occur(list_cgt)

    print("FIN des  croisements")
    Infogreffe().get_turnover_data(infogreffe_etired_year_comp)
    Infogreffe().get_employees_data(infogreffe_etired_year_comp)
    Infogreffe().get_results_data(infogreffe_etired_year_comp)

    Infogreffe().get_2022(liste_data)


if __name__=="__main__":
    main()