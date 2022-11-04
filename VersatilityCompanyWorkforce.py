import os

import pandas as pd

class Versatility:
    def __init__(self):
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"

    def read_data(self):
        data = pd.read_csv(self.path_data_in + "Proximites_Evolutions_Metiers_ponderes.csv", sep=";")
        return data

    def load_naf_rome(self):
        data = pd.read_csv(self.path_data_in + "naf_rome.csv", sep=',')
        data = data[['activity_naf_code', 'rome_code', 'factor']].copy()
        return data

    def versatility_by_naf(self, data_naf_rome):
        data_naf_rome['Weight_versatility'] = data_naf_rome.apply(lambda row: row['Weight_versatility']*row['factor'],
                                                                  axis=1)

        naf_rome = data_naf_rome.groupby(['activity_naf_code'])['Weight_versatility'].sum()
        naf_rome = naf_rome.reset_index()
        naf_rome.rename(columns={'activity_naf_code': 'code'})
        # naf_rome.to_csv(self.path_data_out + "VersatilityCompanyWorkforce.csv", sep=";", index=False)
        return naf_rome

    def count_versatility(self, data):
        data_count_versatility = pd.DataFrame([])
        data_count_versatility['Count_versatility'] = data.groupby(['Target'])['Target'].count()
        data_count_versatility = data_count_versatility.reset_index()
        data = data.merge(data_count_versatility, on=['Target'])
        data['Count_versatility'] = data['Count_versatility'].replace(0, 1)
        return data

    def weight_versatility(self, data):
        data['Weight_versatility'] = data.apply(lambda row: row['Weight']*row['Count_versatility'], axis=1)
        return data

    def sum_versatility(self, data):
        data_sum_versatility = data.groupby(['Source'])['Weight_versatility'].sum()
        data_sum_versatility = data_sum_versatility.reset_index()
        return data_sum_versatility

    def merge_data(self, data_versatility, data_naf):
        data_versatility = data_versatility.rename(columns={'Source': 'rome_code'})
        data_versatility = data_versatility.merge(data_naf, on=['rome_code'])
        return data_versatility

    def main_vcw(self):
        data = self.read_data()
        data_naf_rome = self.load_naf_rome()

        data = self.count_versatility(data)
        data = self.weight_versatility(data)
        data_versatility = self.sum_versatility(data)
        data_versatility = self.merge_data(data_versatility, data_naf_rome)

        data_versatility = self.versatility_by_naf(data_versatility)
        data_versatility.rename(columns={'activity_naf_code': 'code'}, inplace=True)
        print("finish to compute Versatility of company's workforce")
        return data_versatility


