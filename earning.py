import os
import pandas as pd
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


class Earning:
    def __init__(self):
        self.data_in = os.getcwd() + "/data/data_in/"
        self.data_out = os.getcwd() + "/data/data_out/"

    def load_data(self):
        df = pd.read_csv(f"{self.data_in}earning_french_companies_2016_2021.csv", sep=',', dtype={'Siren': str},
                         na_values=['#DIV/0!'])  # .add_prefix('workforce_')
        df.set_index('Siren', inplace=True)
        df = df.add_prefix('earning_')
        df.reset_index(inplace=True)
        df.dropna(subset=['Siren'], inplace=True)
        return df

    @staticmethod
    def preprocessing(df):
        df['earning_PRED_2020'] = df['earning_PRED_2020'].fillna(df['earning_2019'])
        df['earning_PRED_2021'] = df['earning_PRED_2021'].fillna(df['earning_2019'])
        df['earning_PRED_2022'] = df['earning_PRED_2022'].fillna(df['earning_2019'])
        return df

    def compute_variation(self, df):
        df['earning_VARIA2020'] = df.apply(lambda row: self.compute_row_varia(row['earning_2020'],
                                                                              row['earning_PRED_2020']), axis=1)
        df['earning_VARIA2021'] = df.apply(lambda row: self.compute_row_varia(row['earning_2021'],
                                                                              row['earning_PRED_2021']), axis=1)
        df['earning_EVO20_21'] = df['earning_2021'].sub(df['earning_2020'])
        df['earning_EVOPRED_2022'] = df.apply(lambda row: - self.compute_row_varia(row['earning_2021'],
                                                                                   row['earning_PRED_2022']), axis=1)
        return df

    @staticmethod
    def compute_row_varia(turnover, pred):
        if turnover == 0 and pred == 0:
            varia = 0
        elif turnover == 0 and pred != 0:
            varia = pred
        else:
            varia = (turnover - pred)/turnover
        return varia

    def save_data(self, df):
        df.to_csv(f"{self.data_in}/compute_earning_french_companies_2016_2021.csv", sep=';', index=False)

    def main_earning(self):
        data = self.load_data()
        data = self.preprocessing(data)
        data = self.compute_variation(data)
        self.save_data(data)
        return data


if __name__ == '__main__':
    ear = Earning()
    data_ea = ear.main_earning()
