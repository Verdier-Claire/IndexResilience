import os

import pandas as pd

class ResilienceSupplyFlows:
    def __init__(self):
        self.data_in = os.getcwd() + "/data/data_in/"
        self.data_out = os.getcwd() + "/data/data_out/"

    def load_data(self):
        data_naf_hs4 = pd.read_csv(self.data_in + "NAF_HS4.csv", sep=',')
        data_naf_hs4.rename(columns={'NAF': 'code_naf', 'HS4': 'code_hs4', 'weight': 'weight_hs4_naf'}, inplace=True)
        data_naf_hs4['cpf4'] = data_naf_hs4['code_naf'] .apply(lambda row: row[:2] + '.' + row[2:-1])

        data_resilience = pd.read_csv(self.data_in + "ranking_productive_resilience.csv", sep=",")
        data_resilience.rename(columns={'hs4': 'code_hs4'}, inplace=True)

        data_io = pd.read_excel(self.data_in + "intermediate_consumption.xlsx", index_col=0)

        return data_naf_hs4, data_resilience, data_io

    def suppliers_from_naf(self, data_io):
        suppliers = [[str(col), data_io.loc[data_io[col] > 0, col].index.to_list(), data_io.loc[data_io[col] > 0, col].to_list()]
                     for col in data_io.columns]
        data_suppliers = pd.DataFrame(suppliers, columns=['cpf4', 'dest', 'qte'])
        del data_io
        return data_suppliers

    def merge_data(self, data_naf_hs4, data_resilience, data_suppliers):
        data = data_suppliers.merge(data_naf_hs4, on='cpf4')
        data = data.merge(data_resilience, on='code_hs4')
        del data_naf_hs4, data_resilience, data_suppliers
        return data

    def groupby_hs4(self, data):
        data['res_weight_product'] = data.apply(lambda row: row['resilience'] * row['weight_hs4_naf'], axis=1)
        groupby = data.groupby(['cpf4'], group_keys=True)['res_weight_product'].sum().to_frame(name='sum_product').reset_index()
        data = data.merge(groupby, on='cpf4')
        return data

    def compute_index(self, data):
        data = data[['cpf4', 'code_naf', 'qte', 'dest',  'sum_product']].copy()

        resilience_flow = [[data.loc[data['cpf4'].isin(row),
                                      ['cpf4', 'sum_product']].loc[data['cpf4'] == act, 'sum_product'].values * weight,
                            index]
                           if act in data['cpf4'].to_list()
                           else [[0], index]
                           for row, index in zip(data['dest'], data.index)
                           for weight, act in zip(data['qte'][index], data['dest'][index])
                           ]
        data_res = pd.DataFrame(resilience_flow, columns=['resilience_flow', 'index']).groupby(['index']).sum()
        data = pd.merge(data, data_res, left_index=True, right_index=True)
        del data_res

        data_final = data[['code_naf', 'resilience_flow']].copy()
        del data
        data_final.to_csv(self.data_out + "ResilienceSupplyFlows.csv", sep=';', index=False)
        return data_final

    def index_resilience(self):
        data_NAF_HS4, data_resilience, data_supplier = self.load_data()
        data_supplier = self.suppliers_from_naf(data_supplier)
        data = self.merge_data(data_NAF_HS4, data_resilience, data_supplier)
        data = self.groupby_hs4(data)
        data = self.compute_index(data)
        return data