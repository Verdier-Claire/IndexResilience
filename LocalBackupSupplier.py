import os

import pandas as pd
import numpy as np
import itertools


class LocalBackupSuppliers:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"

    def load_turnover_data(self):
        data = pd.read_csv(self.path_data_in + "turnover_with_other_data.csv", sep=";",
                           dtype={"cpf4_x": str, "code_cpf_importation": str, 'departement_importation': str})
        data.rename(columns={'code_cpf_importation': 'Code_CPF4', 'departement_importation': 'Code_depart'},
                    inplace=True)
        return data

    def load_data(self):
        data = pd.read_csv(self.path_data_in + "offices-france.csv", sep=',')
        return data

    def same_company_for_supplier(self, data):
        data_activity = data.copy()
        groupby_cpf4 = data_activity.groupby(['code'])['siret'].apply(list)
        groupby_cpf4 = groupby_cpf4.reset_index()
        groupby_cpf4.rename(columns={'siret': 'list_siren_same_naf'}, inplace=True)
        data_activity = data_activity.merge(groupby_cpf4, on='code')

        data['list_siret_prox_same_naf'] = data_activity.apply(
            lambda row: self.same_supplier_prox(row['siret_prox'], row['list_siren_same_naf']), axis=1)
        data['qte'] = data['qte'].apply(eval)
        data['denominateur'] = data.apply(lambda row: len(row[['list_siret_prox_same_naf']]) * sum(row['qte']), axis=1)

        return data

    def exp_sum_qte_cards(self, list_siret, qte):
        ret = len(list_siret) * np.array(qte)
        ret = ret.sum()
        return ret

    def same_supplier_prox(self, list_dist, list_siret):
        list_siret_prox = list(set(list_dist) & set(list_siret))
        return list_siret_prox

    def nb_local_backup_supplier(self, data):
        data['dest'] = data['dest'].apply(eval)
        data['Card_lbs'] = data.apply(lambda row: self.card_lbs(data, row['dest'], row['siret_prox'], row['qte']),
                                      axis=1)
        return data

    def card_lbs(self, data, dest, dist_siret, qte):
        card_supplier = []
        for activite, weight in zip(dest, qte):
            list_cpf4 = data[data['cpf4'] == activite]
            list_siret = list_cpf4['siret']
            list_lbs = list(set(list_siret) & set(dist_siret))
            if (type(list_lbs) is int) or (list_lbs == []):
                card_supplier.append(weight)  # Dans l'équation finale, on prend le max entre 1 et le card_supplier
            else:
                card_supplier.append(len(list_lbs)*weight)
        ret = sum(card_supplier)
        return ret

    def code_to_cpf4(self, row):
        ret = str(row)
        if len(ret) != 5:
            ret = ret[:-1]
        return ret

    def main_lbs(self, radius=100):

        data_turnover = self.load_turnover_data()
        print('load data_turnover')
        data_turnover = data_turnover[['code', 'dest', 'qte']]
        data_turnover = data_turnover.dropna().drop_duplicates()

        data_office = self.load_data()
        print('load data_office')
        data_office = data_office.merge(data_turnover, on=['code'])  # perte de 7727 entreprises
        del data_turnover
        print('merge data')
        data_office = data_office.sample(1000)
        data_office[['longitude', 'latitude']] = data_office['coordinates'].str.replace(
            '(', '', regex=True).str.replace(')', '', regex=True).str.split(",", 1, expand=True)
        data_office['longitude'] = data_office['longitude'].astype('float')
        data_office['latitude'] = data_office['latitude'].astype('float')
        siret_prox = [[data_office.loc[index1, 'siret'], data_office.loc[index2, 'siret']] for index1, index2
                      in itertools.combinations(data_office.index, 2)
                      if np.sqrt((data_office.loc[index1, 'longitude'] - data_office.loc[index2, 'longitude']) ** 2 +
                                 (data_office.loc[index1, 'latitude'] - data_office.loc[index2, 'latitude']) ** 2) *
                      111.319 <= radius]

        print('finis compute proximities between siret')
        data_siret_prox = pd.DataFrame(siret_prox, columns=['siret', 'siret_prox'])
        data_siret_prox = data_siret_prox.groupby(['siret'])['siret_prox'].apply(list)
        data_siret_prox = data_siret_prox.reset_index()
        data_siret_prox.to_csv(self.path_data_in + "data_siret_prox_" + str(radius) + "km.csv", sep=";", index=False)
        data_office = data_office.merge(data_siret_prox, on=['siret'], how='left')
        data_office['siret_prox'].fillna("", inplace=True)

        data_office['cpf4'] = data_office['code'].apply(lambda row: self.code_to_cpf4(row))

        data_office = self.same_company_for_supplier(data_office)
        print('finish compute same company for supplier')
        data_office = self.nb_local_backup_supplier(data_office)

        data_office['LocalBackupSuppliers'] = data_office.apply(lambda row: row['Card_lbs'] / row['denominateur'],
                                                                axis=1)

        data_final = self.save_data(data_office)
        print('finish to compute Local Backup Supplier')
        return data_final

    def save_data(self, data):
        data_final = data[['siret', 'code', 'LocalBackupSuppliers']]
        # data_final.to_csv(self.path_data_out + "LocalBackupSupplier.csv", sep=';', index=False)
        return data_final
