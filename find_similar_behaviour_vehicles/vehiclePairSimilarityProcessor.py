import pandas as pd
from math import radians, cos, sin, sqrt, atan2
from joblib import Parallel, delayed
from datetime import datetime, timedelta

class VehiclePairSimilarityProcessor:
    def __init__(
            self, 
            df_1, df_2, 
            distance_threshold=0.15, # 位置差の閾値  
            speed_threshold=20,  # 速度差の閾値
            heading_threshold=6,  # 走行方向差の閾値
            similarity_threshold=0.6,  # 類似性の閾値
            std_threshold=0.02, # 標準偏差のばらつきの閾値
            similarity_count_threshold=2, # 類似データの最低個数
            time_tolerance=0,  # 比較対象のタイムスタンプの範囲（変更しないほうがよい）
            weight=[0.5, 0.3, 0.2],  # 重み(distance, speed, heading) 合計 1 になるように設定
            flg_debug=False,
    ):
        try:
            self.DISTANCE_THRESHOLD = distance_threshold
            self.SPEED_THRESHOLD = speed_threshold
            self.HEADING_THRESHOLD = heading_threshold
            self.SIMILARITY_THRESHOLD = similarity_threshold
            self.STD_THRESHOLD = std_threshold
            self.similarity_count_threshold = similarity_count_threshold
            self.TIME_TOLERANCE = time_tolerance
            self.weight = weight
            self.flg_debug = flg_debug

            # Ensure 'Timestamp' columns are converted to datetime
            self.df_1 = df_1.copy()
            self.df_1['Timestamp'] = pd.to_datetime(self.df_1['Timestamp'], errors='raise')

            self.df_2 = df_2.copy()
            self.df_2['Timestamp'] = pd.to_datetime(self.df_2['Timestamp'], errors='raise')

        except Exception as e:
            raise ValueError(f"Error initializing processor: {e}")
    
    # 位置差の計算
    def haversine(self, lat1, lon1, lat2, lon2):
        try:
            R = 6371  # 地球の外周
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            return R * c
        except Exception as e:
            raise ValueError(f"Error calculating haversine distance: {e}")

    # 類似性計算
    def calculate_similarity(self, df_1_row, df_2_row):
        try:
            distance = self.haversine(df_1_row['Latitude'], df_1_row['Longitude'], df_2_row['Latitude'], df_2_row['Longitude'])
            speed_diff = abs(df_1_row['Speed'] - df_2_row['Speed'])
            heading_diff = abs(df_1_row['Heading'] - df_2_row['Heading'])

            distance_score = (1 - min(distance / self.DISTANCE_THRESHOLD, 1)) * self.weight[0]
            speed_score = (1 - min(speed_diff / self.SPEED_THRESHOLD, 1)) * self.weight[1]
            heading_score = (1 - min(heading_diff / self.HEADING_THRESHOLD, 1)) * self.weight[2]

            return distance_score + speed_score + heading_score
        
        except KeyError as e:
            raise KeyError(f"Missing expected column in data: {e}")
        except Exception as e:
            raise ValueError(f"Error calculating similarity: {e}")
        
    # 比較対象のタイムスタンプの範囲の計算処理
    def find_time_range(self, df_1_timestamp):
        try:
            min_timestamp = df_1_timestamp - timedelta(seconds=self.TIME_TOLERANCE)
            max_timestamp = df_1_timestamp + timedelta(seconds=self.TIME_TOLERANCE)
            return self.df_2[(self.df_2['Timestamp'] >= min_timestamp) & (self.df_2['Timestamp'] <= max_timestamp)]
        except KeyError as e:
            raise KeyError(f"Timestamp column not found: {e}")
        except Exception as e:
            raise ValueError(f"Error finding df_2 rows in time range: {e}")

    def process_df(self, df_1_row):
        try:
            similarity_results = []
            df_2_rows_in_range = self.find_time_range(df_1_row['Timestamp'])

            for _, df_2_row in df_2_rows_in_range.iterrows():
                similarity = self.calculate_similarity(df_1_row, df_2_row)
                similarity_results.append({
                    'df_1_VehicleID': df_1_row['VehicleID'],
                    'df_2_VehicleID': df_2_row['VehicleID'],
                    'Similarity': similarity
                })

            return similarity_results
        except Exception as e:
            raise ValueError(f"Error processing df_1 row: {e}")

    def parallel_process_df(self):
        try:
            results = Parallel(n_jobs=-1)(delayed(self.process_df)(df_1_row) for _, df_1_row in self.df_1.iterrows())
            flattened_results = [item for sublist in results for item in sublist]
            return flattened_results
        except Exception as e:
            raise ValueError(f"Error in parallel processing: {e}")
        
    # 各車両のペアの類似性の最終結果（標準偏差のばらつき、中央値、類似しているデータの個数で判断）
    def aggregate_and_filter_results(self, similarity_data):
        try:
            similarity_df = pd.DataFrame(similarity_data)
            aggregated_similarity_df = similarity_df.groupby(['df_1_VehicleID', 'df_2_VehicleID']).agg(
                Median_Similarity=('Similarity', 'median'),
                Std_Similarity=('Similarity', 'std'),
                Count_Similarity=('Similarity', 'count')
            ).reset_index()

            aggregated_similarity_df = aggregated_similarity_df[aggregated_similarity_df['Count_Similarity'] > self.similarity_count_threshold].reset_index(drop=True)

            aggregated_similarity_df['Final_Similarity'] = aggregated_similarity_df.apply(
                lambda row: row['Median_Similarity'] if row['Std_Similarity'] <= self.STD_THRESHOLD and row['Median_Similarity'] >= self.SIMILARITY_THRESHOLD else None,
                axis=1
            )

            final_similarity_df = aggregated_similarity_df.dropna(subset=['Final_Similarity']).reset_index(drop=True)

            if self.flg_debug:
                result = final_similarity_df
            else:
                result = final_similarity_df['df_2_VehicleID'].tolist()
                
            return result
        
        except KeyError as e:
            raise KeyError(f"Error in grouping or column access: {e}")
        except Exception as e:
            raise ValueError(f"Error aggregating and filtering results: {e}")
