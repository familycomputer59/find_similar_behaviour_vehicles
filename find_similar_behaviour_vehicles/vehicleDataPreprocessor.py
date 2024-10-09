import pandas as pd
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod

# 抽象クラス
class VehicleDataPreprocessor(metaclass=ABCMeta):
    def __init__(self, df_1, df_2):
        try:
            self.df_1 = df_1
            self.df_2 = df_2
           
        except Exception as e:
            raise Exception(f"An error occurred while reading the files: {e}")
        
    # ゴミデータの削除
    def clean_data(self):
        try:
            self.df_1 = self.df_1.dropna()
            self.df_1 = self.df_1[
                (self.df_1['Latitude'].between(-90, 90)) &
                (self.df_1['Longitude'].between(-180, 180)) &
                (self.df_1['Speed'].between(0, 200))
            ]

            self.df_2 = self.df_2.dropna()
            self.df_2 = self.df_2[
                (self.df_2['Latitude'].between(-90, 90)) &
                (self.df_2['Longitude'].between(-180, 180)) &
                (self.df_2['Hour'].between(0, 23)) &
                (self.df_2['Speed'].between(1, 60))
            ]
        except KeyError as e:
            raise KeyError(f"One of the required columns is missing: {e}")
        except Exception as e:
            raise ValueError(f"Error cleaning data: {e}")

    # 対象のデータの取得期間をあわせる
    def filter_by_timestamp(self):
        try:
            min_timestamp = (pd.to_datetime(self.df_2['Timestamp']).min() - timedelta(seconds=5)).strftime('%Y%m%d%H%M%S')
            max_timestamp = (pd.to_datetime(self.df_2['Timestamp']).max() + timedelta(seconds=5)).strftime('%Y%m%d%H%M%S')

            self.df_1 = self.df_1[(self.df_1['Timestamp'] >= min_timestamp) & (self.df_1['Timestamp'] <= max_timestamp)]
        except Exception as e:
            raise ValueError(f"Error filtering by timestamp: {e}")
    
    @abstractmethod
    def query_required_data(self):
        pass

    @abstractmethod
    def standardize_datasets(self):
        pass

    def preprocess(self):
        try:
            self.query_required_data()
            self.clean_data()
            self.standardize_datasets()
            self.filter_by_timestamp()

            return self.df_1, self.df_2
        
        except Exception as e:
            print(f"Preprocessing failed: {e}")

# 走行履歴情報 様式１－２ 用の前処理クラス
class VehicleDataPreprocessor_1_2(VehicleDataPreprocessor):
    def __init__(self, df_1, df_2):
        super().__init__(df_1, df_2)

    # 計算に必要なデータのみの取り出し
    def query_required_data(self):
        try:
            self.df_1 = self.df_1[(self.df_1.iloc[:, 17].isin([0, 1]))]
            self.df_1 = self.df_1.iloc[:, [3, 6, 18, 21, 22]]
            self.df_1.columns = ["VehicleID", "Timestamp", "Speed", "Longitude", "Latitude"]

            self.df_2 = self.df_2.iloc[:, [7, 19, 20, 21, 22, 23, 27]]

        except Exception as e:
            raise ValueError(f"Error querying required data: {e}")
        
        # 計算に適した数値へ最適化
    def standardize_datasets(self):
        try:
            self.df_1['Timestamp'] = "20000101" + pd.to_datetime(self.df_1['Timestamp'], format='%Y%m%d%H%M%S').dt.strftime('%H%M%S')
            # self.df_1[['Latitude', 'Longitude']] = self.df_1[['Latitude', 'Longitude']].round(6)
            self.df_1['Speed'] = self.df_1['Speed'].astype(float)
            # self.df_1['Heading'] = self.df_1['Heading'].astype(int)
            
            self.df_2['Timestamp'] = "20000101" + \
                                     self.df_2['Hour'].astype(str).str.zfill(2) + \
                                     self.df_2['Minute'].astype(str).str.zfill(2) + \
                                     self.df_2['Second'].apply(lambda x: f"{x:06.3f}").str[:2]
            self.df_2['Speed'] = (self.df_2['Speed'] * 3.6).round(2)
            # self.df_2['Heading'] = (self.df_2['Heading'] // 24).astype(int)
            # self.df_2[['Latitude', 'Longitude']] = self.df_2[['Latitude', 'Longitude']].round(6)
            self.df_2 = self.df_2.drop(columns=['Hour', 'Minute', 'Second'])

        except KeyError as e:
            raise KeyError(f"Latitude or Longitude columns are missing: {e}")
        except Exception as e:
            raise ValueError(f"Error standardizing timestamps: {e}")

# 挙動履歴情報 様式１－４ 用の前処理クラス 
class VehicleDataPreprocessor_1_4(VehicleDataPreprocessor):
    def __init__(self, df_1, df_2):
        super().__init__(df_1, df_2)

    # 計算に必要なデータのみの取り出し
    def query_required_data(self):
        try:
            self.df_1 = self.df_1[(self.df_1.iloc[:, 10].isin([0, 1]))]
            self.df_1 = self.df_1.iloc[:, [2, 6, 7, 8, 9, 14]]
            self.df_1.columns = ["VehicleID", "Timestamp", "Longitude", "Latitude", "Heading", "Speed"]

            self.df_2 = self.df_2.iloc[:, [7, 19, 20, 21, 22, 23, 27, 28]]
            
        except Exception as e:
            raise ValueError(f"Error querying required data: {e}")
        
    # 計算に適した数値へ最適化
    def standardize_datasets(self):
        try:
            self.df_1['Timestamp'] = "20000101" + pd.to_datetime(self.df_1['Timestamp'], format='%Y%m%d%H%M%S').dt.strftime('%H%M%S')
            self.df_1['Speed'] = self.df_1['Speed'].astype(float)
            self.df_1['Heading'] = self.df_1['Heading'].astype(int)
            
            self.df_2['Timestamp'] = "20000101" + \
                                     self.df_2['Hour'].astype(str).str.zfill(2) + \
                                     self.df_2['Minute'].astype(str).str.zfill(2) + \
                                     self.df_2['Second'].apply(lambda x: f"{x:06.3f}").str[:2]
            self.df_2['Speed'] = (self.df_2['Speed'] * 3.6).round(2)
            self.df_2['Heading'] = (self.df_2['Heading'] // 24).astype(int)
            self.df_2 = self.df_2.drop(columns=['Hour', 'Minute', 'Second'])

        except KeyError as e:
            raise KeyError(f"Latitude or Longitude columns are missing: {e}")
        except Exception as e:
            raise ValueError(f"Error standardizing timestamps: {e}")
        