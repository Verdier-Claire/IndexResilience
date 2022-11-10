import os
import pandas as pd


#  need data avec entreprise relié à leur code produit


class AgilityCompanyProduct:
    def __init__(self):
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + '/data/data_out/'

    def sum_weight_by_product_b(self, data):
        data_sum_prox_productB = data.groupby(['Product_B'])['weight'].sum()
        data_sum_prox_productB = data_sum_prox_productB.reset_index()
        data_sum_prox_productB = data_sum_prox_productB.rename(columns={'weight': 'sum_prox_by_product_B'})

        data = data.merge(data_sum_prox_productB, on=['Product_B'])
        return data

    def weight_by_product(self, weight, sum_weight):
        if sum_weight == 0:
            ret = 0
        else:
            ret = weight / sum_weight
        return ret

    def sum_by_product_interest(self, data):
        data_product_interest = data.groupby(['Product_A'])['sum_prox_by_product_B'].sum()
        data_product_interest = data_product_interest.reset_index()
        data_product_interest = data_product_interest.rename(columns={'sum_prox_by_product_B':
                                                                          "sum_prox_for_product_A"})
        data = data.merge(data_product_interest, on=['Product_A'])
        return data

    def weight_prox_product(self, data):
        data['versatility_by_product_and_NAF'] = data.apply(lambda row: row['sum_prox_for_product_A'] * row['coef'],
                                                            axis=1)
        return data

    def agility_by_naf(self, data):
        data_agility = data.groupby(['code_NAF'])['versatility_by_product_and_NAF'].sum()
        data_agility = data_agility.reset_index()
        data_agility.rename(columns={'code_NAF': 'code', 'versatility_by_product_and_NAF': 'agility_production'},
                            inplace=True)
        data_agility.to_csv(self.path_data_out + "AgilityCompanyProduction.csv", sep=";", index=False)
        return data_agility

    def main_acp(self):
        ppagility = PreprocessAgility()
        data_agility = ppagility.load_data()
        data_agility = ppagility.similarity_weight(data_agility)

        data_agility = self.sum_weight_by_product_b(data_agility)
        data_agility['sum_prox_by_product_B'] = data_agility.apply(lambda row: self.weight_by_product(
            row['weight'], row['sum_prox_by_product_B']), axis=1)
        data_agility = self.sum_by_product_interest(data_agility)
        data_agility = data_agility[['Product_A', 'sum_prox_for_product_A']]
        data_agility = data_agility.drop_duplicates()

        codehscpf4 = CodeHSCPF()
        data_hs_cpf4 = codehscpf4.load_hs_cpf4()
        data_naf_cpf4 = codehscpf4.load_naf_cpf6()
        data_naf_cpf4 = codehscpf4.preprocess(data_naf_cpf4)
        data_hs_cpf4 = codehscpf4.merge(data_hs_cpf4, data_naf_cpf4)
        data_hs_naf = codehscpf4.data_interest(data_hs_cpf4)
        del data_hs_cpf4, data_naf_cpf4

        data_hs_naf = data_hs_naf.rename(columns={'hs4': 'Product_A'})
        data = data_agility.merge(data_hs_naf, on=['Product_A'])

        data = self.weight_prox_product(data)
        data_final = self.agility_by_naf(data)
        del data

        print("finish to compute Agility of Company's Production")
        data_final.rename(columns={'code_NAF': 'code'}, inplace=True)
        return data_final


class PreprocessAgility:
    def __init__(self):
        self.path_data_in = os.getcwd() + "/data/data_in/"


    def load_data(self):
        data = pd.read_csv(self.path_data_in + "HS_similarity_usingCF.csv", sep=',', header=None,
                           names=['Product_A', 'Product_B', 'weight'],
                           dtype={'Product_A': str, 'Product_B': str})
        return data

    def similarity_weight(self, data):
        data['weight'] = 1 - data['weight']
        return data


class CodeHSCPF:
    def __init__(self):
        self.path_data_in = os.getcwd() + "/data/data_in/"

    def load_hs_cpf4(self):
        data = pd.read_csv(self.path_data_in + 'code_HS4_CPF4.csv', sep=',', dtype={"hs4": str, "cpf4": str})
        return data

    def load_naf_cpf6(self):
        data = pd.read_csv(self.path_data_in + "code_NAF_CPF6.csv", sep=',', dtype={"code_a732": str, "code_cpf6": str})
        return data

    def preprocess(self, data):
        data['code_cpf6'] = data.apply(lambda row: row['code_cpf6'][:4], axis=1)
        data = data.rename(columns={'code_cpf6': 'cpf4', 'code_a732': 'code_NAF'})
        return data

    def merge(self, data_hs_cpf4, data_naf_cpf4):
        data = data_hs_cpf4.merge(data_naf_cpf4, on=['cpf4'])
        return data

    def data_interest(self, data):
        data = data[['hs4', 'coef', 'code_NAF']].copy()
        data = data.drop_duplicates()
        return data



