#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 09:31:29 2022

@author: nsauzeat
"""
import psycopg2
import pandas as pd
import os


class Analysis:

    def __init__(self):
        self.path = 'C:\\Users\\OpenStudio.Aurora-R13\\Desktop\\IndexResilience'
        self.path_data_in = f"{self.path}/data/data_in"
        self.path_data_infogreffe = f"{self.path_data_in}/data_infogreffe"
        self.path_data_out = f"{self.path}/data/data_out/"
        self.drop_col_list = ["address_id", "company_id", "main_activity_id", "secondary_address_id", "updated_by",
                              'phone_fix', 'phone_mobile', 'it_code', 'sector_id', 'id', 'city_id', 'macro_sector_id',
                              'parent_activity_id', 'section_id', 'macro_sector_naf_id', 'department_id',
                              'industry_territory_id', 'perimeter', "deleted_at", "updated_at", "created_at", "z_val",
                              "y_val"]

        self.conn =  psycopg2.connect(
           user = "iat",
           password = "bR3fTAk2VkCNbDPg",
           host = "localhost",
           port = "6432",
           database = "iat"
              )
        self.cur =  self.conn.cursor()


    def  read_data(self):
        all_turnover = pd.read_csv(self.path_data_in +"/turnover_french_companies_2016_2021.csv",sep=",",dtype={"Siren":str})
        all_results =pd.read_csv(self.path_data_in+"/earning_french_companies_2016_2021.csv",sep=",",dtype={"Siren":str})
        all_employees =pd.read_csv(self.path_data_in+"/workforce_french_companies_2016_2021.csv",sep=",",dtype={"Siren":str})
        return all_turnover, all_results, all_employees

    def cross_turnover_atlas(self,turnover):
        siren_turnover = turnover["Siren"]
        atlas_estab = self.recup_estab_atlas(siren_turnover)
        atlas_estab["Siren"] = atlas_estab["siret"].apply(lambda x: x[:9])
        atlas_turnover = turnover.set_index("Siren").join(atlas_estab.set_index("Siren"))
        atlas_turnover.drop(columns=self.drop_col_list, inplace=True)

        atlas_turnover.to_csv(f"{self.path_data_out}/extract_turnover_cross_atlas.csv", sep=";")


    def recup_estab_atlas(self, siren_turnover):
            sql = """WITH sel_raw as( SELECT * FROM establishment 
                            WHERE LEFT(siret,9) IN {}
                            )
                    SELECT * FROM sel_raw
                    LEFT JOIN (SELECT address.id, address.way_label, address.way_number, address.way_type, address.y_val, address.z_val, address.cedex, address.cedex_label,
							   address.city_id, address.special_distribution  FROM address
							) AS B  on B.id = sel_raw.address_id                   
                    LEFT JOIN nomenclature_activity na on na.id = sel_raw.main_activity_id
                    LEFT JOIN city on city.id =B.city_id """.format(tuple(siren_turnover))
            self.cur.execute(sql)
            atlas_estab = self.cur.fetchall()
            colnames = [desc[0] for desc in self.cur.description]
            self.conn.commit()
            atlas_estab =  pd.DataFrame(atlas_estab, columns=colnames)
            return atlas_estab


    def cross_results_atlas(self,results):
        siren_results =results["Siren"]
        atlas_estab = self.recup_estab_atlas(siren_results)
        atlas_estab["Siren"] = atlas_estab["siret"].apply(lambda x: x[:9])
        atlas_results = results.set_index("Siren").join(atlas_estab.set_index("Siren"))
        atlas_results.drop(columns=self.drop_col_list, inplace=True)

        atlas_results.to_csv(f"{self.path_data_out}/extract_results_cross_atlas.csv", sep=";")



Analysis= Analysis()

def main():
    all_turnover,all_results,all_employees = Analysis.read_data()
    Analysis.cross_turnover_atlas(all_turnover)
    Analysis.cross_results_atlas(all_results)


main()