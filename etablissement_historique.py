import pathlib
import os

import pandas as pd


class Etablissementistorique:
        def __init__(self):
            self.data_in = os.getcwd() + "/data/data_in/"
            self.data_out = os.getcwd() + "/data/data_out/"

        def load_data(self):
            data = pd.read_csv(f"{self.data_in}/StockEtablissementHistorique_utf8.csv", sep=';',
                               dtype={'nic': str, 'siren': str, 'siret': str})
            return data

        def __call__(self, *args, **kwargs):
            data = self.load_data()

            # select etablissement employeur (remove etablissement non employeur)
            data = data.loc[data['caractereEmployeurEtablissement'] != 'N']

            list_col_to_keep = ['activitePrincipaleEtablissement', 'ChangementActivitePrincipaleEtablissement',
                                'changementEtatAdministratifEtablissement', 'etatAdministratifEtablissement',
                                'dateDebut', 'dateFin',
                                'nic', 'siren', 'siret']

            data = data[list_col_to_keep].copy()

