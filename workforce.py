import os
import time
import pandas as pd
from pandas.plotting import register_matplotlib_converters
# Modeling and Forecasting
# ==============================================================================
from sklearn.ensemble import RandomForestRegressor
from skforecast.ForecasterAutoreg import ForecasterAutoreg
register_matplotlib_converters()


class Workforce:
    def __init__(self):
        self.data_in = os.getcwd() + "/data/data_in/"
        self.data_out = os.getcwd() + "/data/data_out/"

    def load_data(self, compute=False):
        if compute:
            df = pd.read_csv(f"{self.data_in}workforce_french_companies_2016_2021.csv", sep=',', dtype={'Siren': str})
            df.set_index('Siren', inplace=True)
        else:
            df = pd.read_csv(f"{self.data_in}workforce_pred_2016_2021.csv", sep=';', dtype={'Siren': str})
        return df

    @staticmethod
    def sarima_method(row, year):
        df_st = pd.DataFrame(row)
        df_st.reset_index(inplace=True)
        df_st.columns = ['date', 'workforce']
        if year =='2020':
            step = 4
        elif year == '2021':
            step = 5
        else:
            step = 6

        data_train = df_st[:-step].copy()

        forecaster = ForecasterAutoreg(
            regressor=RandomForestRegressor(random_state=3),
            lags=1
        )
        forecaster.fit(y=data_train['workforce'])
        prediction = forecaster.predict(steps=1)

        return prediction.values[0]

    def main_workforce(self, compute=False):
        data = self.load_data(compute=False)
        if compute:
            data = self.fit_linear_regression(data)

        return data


if __name__ == '__main__':
    wf = Workforce()
    data_wf = wf.main_workforce()
