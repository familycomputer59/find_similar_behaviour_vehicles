
# Sample Usage
## install
```
poetry add git+https://github.com/familycomputer59/find_similar_behaviour_vehicles
```

## Sample Code
```
import vehicleDataPreprocessor as vd
import vehiclePairSimilarityPocessor as vsp
import pandas as pd

# ETC2.0 データ想定
file_path_1 = './data/data.csv'
df_1 = pd.read_csv(file_path_1)

# ITS Connect データ想定
file_path_2 = './data/output_231208_4.csv'
df_2 = pd.read_csv(file_path_2)

preprocessor = vd.VehicleDataPreprocessor(df_1, df_2)
preprocced_df_1, preprocced_df_2 = preprocessor.preprocess()

print("preprosessing finished")

flg_debug = True

processor = vsp.VehiclePairSimilarityProcessor(preprocced_df_1, preprocced_df_2,
            distance_threshold=0.15, # 位置差の閾値  
            speed_threshold=20,  # 速度差の閾値
            heading_threshold=6,  # 走行方向差の閾値
            similarity_threshold=0.6,  # 類似性の閾値
            std_threshold=0.02, # 標準偏差のばらつきの閾値
            similarity_count_threshold=2, # 類似データの最低個数
            time_tolerance=0,  # 比較対象のタイムスタンプの範囲（変更しないほうがよい）
            weight=[0.5, 0.3, 0.2],  # 重み(distance, speed, heading) 合計 1 になるように設定
            flg_debug=flg_debug # Debug フラグ
        )

similarity_data = processor.parallel_process_df()
print("excute processing")

result = processor.aggregate_and_filter_results(similarity_data)
print("finalyezed data")

if flg_debug:
    result.to_csv('out/Final_Similarity_Results_Filtered.csv', index=False)
else:
    df_filtered = df_2[df_2['VehicleID'].isin(result)]
    df_filtered.to_csv('out/extract_itsdata.csv', index=False)

```
