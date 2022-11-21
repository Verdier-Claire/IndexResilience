import os
import numpy as np
import pandas as pd

class ResilienceSupplyFlows:
    def __init__(self):
        self.data_in = os.getcwd() + "/data/data_in/"
        self.data_out = os.getcwd() + "/data/data_out/"

    def load_data(self):
        pd.set_option('display.float_format', '{:.12E}'.format)
        data_office = pd.read_csv("/Users/cverdier/Documents/ModeleEvolutionEntreprise/IndexResilienceArnault/"
                                  "Resilience_Index/input/offices-france.csv", sep=',')
        data_office['code_cpf4'] = data_office['code'].apply(lambda row: str(row[0:5]))
        data_office.rename(columns={'code': 'code_naf'}, inplace=True)

        data_naf_hs4 = pd.read_csv("/Users/cverdier/Documents/ModeleEvolutionEntreprise/IndexResilienceArnault/"
                                   "Resilience_Index/input/NAF_HS4.csv", sep=',', )
        data_naf_hs4.rename(columns={'NAF': 'code_naf', 'HS4': 'code_hs4', 'weight': 'weight_hs4_naf'}, inplace=True)
        data_naf_hs4['code_naf'] = data_naf_hs4['code_naf'] .apply(lambda row: row[:2] + '.' + row[2:])
        data_naf_hs4['code_cpf4'] = data_naf_hs4['code_naf'].apply(lambda row: row[:5])

        data_resilience = pd.read_csv("/Users/cverdier/Documents/ModeleEvolutionEntreprise/IndexResilienceArnault/"
                                      "Resilience_Index/input/ranking_productive_resilience.csv", sep=",",
                                      dtype={'resilience': 'float64'})
        data_resilience.rename(columns={'hs4': 'code_hs4'}, inplace=True)

        # data_io = pd.read_excel(self.data_in + "intermediate_consumption.xlsx", index_col=0)
        data_io = pd.read_csv("/Users/cverdier/Documents/ModeleEvolutionEntreprise/IndexResilienceArnault/"
                              "Resilience_Index/input/IO-matrix.csv", sep=";", index_col=0)
        data_io.set_index(data_io.columns, inplace=True)
        return data_office, data_naf_hs4, data_resilience, data_io

    def suppliers_from_naf(self, data_io):
        data_io = data_io.transpose()
        suppliers = [[str(col), data_io.loc[data_io[col] > 0, col].index.to_list(),
                      data_io.loc[data_io[col] > 0, col].to_list()]
                     for col in data_io.index]
        data_suppliers = pd.DataFrame(suppliers, columns=['code_cpf4', 'dest', 'qte'])
        del data_io
        return data_suppliers

    def merge_data_hs4(self, data_naf_hs4, data_resilience):
        # data = data.merge(data_naf_hs4, on='code_naf')
        # data = data.merge(data_suppliers, on='cpf4')
        data_product = data_naf_hs4.merge(data_resilience, on='code_hs4')
        # del data_naf_hs4, data_resilience, data_suppliers
        return data_product

    def groupby_hs4(self, data):
        data['res_weight_product'] = data.apply(lambda row: row['resilience_nomalized'] * row['weight_hs4_naf'], axis=1)
        groupby = data.groupby(['code_cpf4'], group_keys=True)['res_weight_product'].sum().to_frame(name='sum_product').reset_index()
        data = data.merge(groupby, on='code_cpf4')
        data = data[['code_naf', 'code_cpf4', 'sum_product']]
        # data['code_cpf4'] = data['code_naf'].apply(lambda row: row[:5])
        return data

    def compute_index(self, data):
        resilience_flow = [[(data.loc[data['code_cpf4'].isin(row),
                                      ['code_cpf4', 'sum_product']].loc[data['code_cpf4'] == act, 'sum_product'].values
                             * weight)[0], data['code_cpf4'][index]]
                           if act in data['code_cpf4'].to_list()
                           else [0, index]
                           for row, index in zip(data['dest'], data.index)
                           for weight, act in zip(data['qte'][index], data['dest'][index])
                           ]


        data_res = pd.DataFrame(resilience_flow, columns=['resilience_flow', 'code_cpf4']).groupby(['code_cpf4']).sum(
             ).reset_index()
        data_res['resilience_flow'] = data_res['resilience_flow'].apply(lambda row: min(row * 10, 10))
        data = pd.merge(data, data_res, on='code_cpf4') #  left_index=True, right_index=True)
        del data_res

        data_final = data[['code_naf', 'resilience_flow']].copy()
        del data
        return data_final

    def index_resilience(self):
        data_office, data_naf_hs4, data_resilience, data_supplier = self.load_data()
        print("load data")
        data_supplier = self.suppliers_from_naf(data_supplier)
        print("compute suppliers by naf")
        data = data_naf_hs4.merge(data_resilience, on='code_hs4')
        del data_naf_hs4, data_resilience
        print("merge datas")
        data = self.groupby_hs4(data)
        print("compute weight hs4 by naf")
        data['code_cpf4'] = data['code_naf'].apply(lambda row: row[:5])
        data = data.merge(data_supplier, on='code_cpf4')
        data = self.compute_index(data)

        data = data.merge(data_office, on='code_naf')
        data.to_csv(self.data_out + "ResilienceSupplyFlows.csv", sep=';', index=False)
        return data
