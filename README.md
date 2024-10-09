
# Installation
```
poetry add git+https://github.com/familycomputer59/find_similar_behaviour_vehicles
```
# Sample Code
## 走行履歴情報 様式１－２ データ
```
import find_similar_behaviour_vehicles.vehicleDataPreprocessor as vd
import find_similar_behaviour_vehicles.vehiclePairSimilarityProcessor as vsp
import pandas as pd

flg_debug = False

# ETC2.0 データ想定(走行履歴情報 様式１－２ データ)
file_path_1 = 'data/data_1-2.csv'
df_1 = pd.read_csv(file_path_1)

# ITS Connect データ想定
file_path_2 = './data/output_231208_4.csv'
df_2 = pd.read_csv(file_path_2)

preprocessor = vd.VehicleDataPreprocessor_1_2(df_1, df_2)
preprocced_df_1, preprocced_df_2 = preprocessor.preprocess()

print("preprosessing finished")

if flg_debug:
    preprocced_df_1.to_csv('out/etc2_data.csv', index=False)
    preprocced_df_2.to_csv('out/its_data.csv', index=False)

processor = vsp.VehiclePairSimilarityProcessor(preprocced_df_1, preprocced_df_2,
            distance_threshold=0.3, # 位置差の閾値  
            speed_threshold=25,  # 速度差の閾値
            similarity_threshold=0.6,  # 類似性の閾値
            std_threshold=0.02, # 標準偏差のばらつきの閾値
            similarity_count_threshold=2, # 類似データの最低個数
            time_tolerance=0,  # 比較対象のタイムスタンプの範囲（変更しないほうがよい）
            weight=[0.7, 0.3],  # 重み(distance, speed, heading) 合計 1 になるように設定
            flg_debug=flg_debug # Debug フラグ
        )

similarity_data = processor.parallel_process_df()
print("excute processing")

if flg_debug:
    similarity_df = pd.DataFrame(similarity_data)
    similarity_df.to_csv('out/similarity_data.csv', index=False)

result = processor.aggregate_and_filter_results(similarity_data)
print("finalyezed data")

if flg_debug:
    result.to_csv('out/debbuging.csv', index=False)
else:
    df_filtered = df_2[df_2['VehicleID'].isin(result)]
    df_filtered.to_csv('out/extracted_data.csv', index=False)
    df_filtered_2 = preprocced_df_2[~preprocced_df_2['VehicleID'].isin(result)]
    df_filtered_2.to_csv('out/unextracted_data.csv', index=False)
```

## 挙動履歴情報 様式１－４ データ
```code
import find_similar_behaviour_vehicles.vehicleDataPreprocessor as vd
import find_similar_behaviour_vehicles.vehiclePairSimilarityProcessor as vsp
import pandas as pd

flg_debug = False

# ETC2.0 データ想定(挙動履歴情報)
file_path_1 = 'data/data_1-4.csv'
df_1 = pd.read_csv(file_path_1)

# ITS Connect データ想定
file_path_2 = './data/output_231208_4.csv'
df_2 = pd.read_csv(file_path_2)

preprocessor = vd.VehicleDataPreprocessor_1_4(df_1, df_2)
preprocced_df_1, preprocced_df_2 = preprocessor.preprocess()

print("preprosessing finished")

if flg_debug:
    preprocced_df_1.to_csv('out/etc2_data.csv', index=False)
    preprocced_df_2.to_csv('out/its_data.csv', index=False)

processor = vsp.VehiclePairSimilarityProcessorWithHeading(preprocced_df_1, preprocced_df_2,
            distance_threshold=0.3, # 位置差の閾値  
            speed_threshold=25,  # 速度差の閾値
            heading_threshold=6,  # 走行方向差の閾値
            similarity_threshold=0.5,  # 類似性の閾値
            std_threshold=0.02, # 標準偏差のばらつきの閾値
            similarity_count_threshold=2, # 類似データの最低個数
            time_tolerance=0,  # 比較対象のタイムスタンプの範囲（変更しないほうがよい）
            weight=[0.5, 0.3, 0.2],  # 重み(distance, speed, heading) 合計 1 になるように設定
            flg_debug=flg_debug # Debug フラグ
        )

similarity_data = processor.parallel_process_df()
print("excute processing")

if flg_debug:
    similarity_df = pd.DataFrame(similarity_data)
    similarity_df.to_csv('out/similarity_data.csv', index=False)

result = processor.aggregate_and_filter_results(similarity_data)
print("finalyezed data")

if flg_debug:
    result.to_csv('out/debbuging.csv', index=False)
else:
    df_filtered = df_2[df_2['VehicleID'].isin(result)]
    df_filtered.to_csv('out/extracted_data.csv', index=False)
    df_filtered_2 = preprocced_df_2[~preprocced_df_2['VehicleID'].isin(result)]
    df_filtered_2.to_csv('out/unextracted_data.csv', index=False)
```
