import os
import geopy.distance
import pandas as pd
import numpy as np
import itertools


class LocalBackupSuppliers:
    def __init__(self):
        self.path = os.getcwd()
        self.path_data_in = os.getcwd() + "/data/data_in/"
        self.path_data_out = os.getcwd() + "/data/data_out/"

    def load_data(self):
        data = pd.read_csv(self.path_data_in + "data-coordinates.csv", sep=',')
        data['coordinates'].fillna(data['coordinates-2'], inplace=True)
        data.drop(['coordinates-2'], inplace=True, axis=1)
        data['code_cpf4'] = data['code'].apply(lambda row: str(row[:5]))

        data_suppliers = pd.read_csv(self.path_data_in + "data_suppliers.csv", sep=';')
        data = data.merge(data_suppliers, on='code_cpf4', how='left')
        del data_suppliers
        data.dropna(inplace=True)
        data.drop_duplicates(inplace=True)
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
        data_office = self.load_data()
        print('load data')
        data_office.sort_values('code', ascending=True, inplace=True)
        data_office = data_office.sample(5000, random_state=3)

        data_office['coordinates'] = data_office['coordinates'].apply(eval)
        data_office['dest'] = data_office['dest'].apply(eval)
        data_office['qte'] = data_office['qte'].apply(eval)

        siret_prox = [self.weight_index(data_office, index1, index2) for index1, index2 in
                      itertools.permutations(data_office.index, 2)]

        print('finis compute proximities between siret')

        data_siret_prox = pd.DataFrame(siret_prox, columns=['siret', 'weight', 'dist', 'supplier', 'same_activite',
                                                            'code_supplier'])

        data_supplier = data_siret_prox.query("supplier == 'True'")
        data_supplier['dist'] = data_supplier['dist', 'weight'].apply(lambda row: row['dist'] * row['weight'], axis=1)
        data_supplier = data_supplier.groupby(['siret', 'code_supplier'])['dist'].sum().reset_index()

        data_rival = data_siret_prox.query("same_activite == 'True'")
        del data_siret_prox
        data_rival = [[data_rival.loc[index, 'siret'], act, data_rival.loc[index, 'dist']] for index in data_rival.index
                      for act in data_rival.loc[index, 'code_supplier']]
        data_rival = pd.DataFrame(data_rival, columns=['siret', 'code_supplier', 'dist_rival']).groupby(['siret',
                                                                                                   'code_supplier'
                                                                                                   ])['dist_rival'].\
            sum().reset_index()
        data_rival = data_supplier.merge(data_rival, on=['siret', 'code_supplier'], how='left')
        data_rival['dist_rival'] = data_rival['dist_rival'].fillna(1)
        data_rival['dist'] = data_rival['dist'].fillna(0)
        del data_supplier
        data_rival['dist'] = data_rival.apply(lambda row: row['dist']/row['dist_rival'], axis=1)
        data_rival = data_rival.groupby(['siret'])['dist'].sum().reset_index()
        # data_siret_prox.to_csv(self.path_data_in + "data_siret_prox_" + str(radius) + "km.csv", sep=";", index=False)
        data_office = data_office.merge(data_supplier, on=['siret'], how='left')
        data_office = data_office.merge(data_rival, on=['siret'], how='left')

        data_office['cpf4'] = data_office['code'].apply(lambda row: self.code_to_cpf4(row))

        data_office = self.same_company_for_supplier(data_office)
        print('finish compute same company for supplier')
        data_office = self.nb_local_backup_supplier(data_office)

        data_office['LocalBackupSuppliers'] = data_office.apply(lambda row: row['Card_lbs'] / row['denominateur'],
                                                                axis=1)

        data_final = self.save_data(data_office)
        print('finish to compute Local Backup Supplier')
        return data_final

    def weight_index(self, data, index1, index2):
        if data.loc[index2, 'code'] in data.loc[index1, 'dest']:
            siret = data.loc[index1, 'siret']
            weight = data.loc[index1, 'qte'][data.loc[index1, 'dest'].index(data.loc[index2, 'code'])]

            dist = geopy.distance.geodesic(data.loc[index1, 'coordinates'], data.loc[index2, 'coordinates']).km
            if dist == 0:
                dist = 1
            dist = 1/dist
            supplier = True
            same_act = data.loc[index1, 'code'] == data.loc[index2, 'code']
            list_same_supplier = data.loc[index2, 'code']
            return [siret, weight, dist, supplier, same_act, list_same_supplier]
        elif data.loc[index1, 'code'] == data.loc[index2, 'code'] or (list(set(data.loc[index1, 'dest']) &
                                                                           set(data.loc[index2, 'dest'])) != []):
            siret = data.loc[index1, 'siret']
            weight = 1
            dist = geopy.distance.geodesic(data.loc[index1, 'coordinates'], data.loc[index2, 'coordinates']).km
            if dist == 0:
                dist = 1
            dist = 1/dist
            supplier = False
            same_act = True
            list_same_supplier = list(set(data.loc[index1, 'dest']) & set(data.loc[index2, 'dest']))
            return [siret, weight, dist, supplier, same_act, list_same_supplier]


    def save_data(self, data):
        data_final = data[['siret', 'code', 'LocalBackupSuppliers']]
        # data_final.to_csv(self.path_data_out + "LocalBackupSupplier.csv", sep=';', index=False)
        return data_final
