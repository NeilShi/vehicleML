import pandas as pd
import vidlist
import time

pd.set_option('display.max_columns', None)
vehicle_data = pd.read_csv('sample_sh50_time_1.csv',
                           usecols=['vid', 'daq_time', 'speed', 'mileage', 'status'],
                           iterator=True, low_memory=False)
chunkSize = 200000
chunks = []
loop = True
driving_df = pd.DataFrame(columns=['vid', 'daq_time', 'speed', 'mileage', 'status'])
stalled_df = pd.DataFrame(columns=['vid', 'daq_time', 'speed', 'mileage', 'status'])
vehicle_summary_df = pd.DataFrame(columns=['vid', 'over_speed_rate', 'morning_and_evening_peak_rate', 'max_mileage',
                                  'driving_mileage', 'night_driving_rate', 'start_up_rate'])
vid_list = vidlist.get_vid_map()


def clear_invalid_data(df):
    # 注意在这里isin([])里的参数都是str类型
    df = df[~df['speed'].isin(['\\N'])]
    df = df[~df['mileage'].isin(['\\N'])]
    df = df[~df['status'].isin(['\\N'])]
    return df


# 添加行驶时段新特征  早晚高峰(7-9, 17-20)：1, 夜间(22-0, 0-6): 2, 其他: 0
def get_period_of_time(row):
    row = str(row)
    if row[8] == '0':
        hour = int(row[9])
    else:
        hour = int(row[8:10])
    if 7 <= hour < 9 or 17 <= hour < 20:
        return 1
    elif 22 <= hour or hour < 6:
        return 2
    else:
        return 0


# 从时间排序后的最后一行数据获取最大里程
def get_max_mileage(df):
    max_mileage = df.iloc[-1]['mileage']
    return max_mileage


def get_driving_mileage(df):
    driving_mileage = df.iloc[-1]['mileage'] - df.iloc[1]['mileage']
    # print('from:%d, to:%d' % (df.iloc[1]['mileage'], df.iloc[-1]['mileage']))
    return driving_mileage


def translate_field_type(df):
    df['speed'] = df['speed'].apply(lambda row: int(row))
    df['mileage'] = df['mileage'].apply(lambda row: int(row))
    return df


def get_over_speed_rate(df, vid_type):
    origin_len = len(df)
    if vid_type == '公':
        df = df.loc[df['speed'] > 40]
    else:
        df = df.loc[df['speed'] > 80]
    # print('origin:%d, over speed:%d' % (origin_len, len(df)))
    return round(len(df)/origin_len, 4)


def get_morning_and_evening_peak_rate(df):
    origin_len = len(df)
    df = df.loc[df['period_of_time'] == 1]
    # print('origin:%d, mor and eve:%d' % (origin_len, len(df)))
    return round(len(df)/origin_len, 4)


def get_night_driving_rate(df):
    origin_len = len(df)
    df = df.loc[df['period_of_time'] == 2]
    # print('origin:%d, night:%d' % (origin_len, len(df)))
    return round(len(df) / origin_len, 4)


def get_start_up_rate(df1, df2):
    return round(len(df1)/(len(df1) + len(df2)), 4)


def generate_summary_per_vid(vid):
    global chunk, driving_df, stalled_df, vehicle_summary_df
    vid_type = vid[2]
    for chunk in chunks:
        chunk_grouped = chunk.groupby('vid')
        try:
            status_grouped = chunk_grouped.get_group(vid).groupby('status')
            driving_df = pd.concat([driving_df, status_grouped.get_group('1')], axis=0, sort=False)
            stalled_df = pd.concat([stalled_df, status_grouped.get_group('2')], axis=0, sort=False)
        except KeyError:
            continue

    driving_df = driving_df.sort_values(by='daq_time', ascending=True)
    driving_df['period_of_time'] = driving_df['daq_time'].apply(get_period_of_time)
    driving_df = translate_field_type(driving_df)
    # 排除里程异常值
    driving_df = driving_df.loc[driving_df['mileage'] <= get_max_mileage(driving_df)]
    summary_df_by_vid = pd.DataFrame([[vid,
                                       get_over_speed_rate(driving_df, vid_type),
                                       get_morning_and_evening_peak_rate(driving_df),
                                       get_max_mileage(driving_df), get_driving_mileage(driving_df),
                                       get_night_driving_rate(driving_df), get_start_up_rate(driving_df, stalled_df)]],
                                     columns=['vid', 'over_speed_rate', 'morning_and_evening_peak_rate', 'max_mileage',
                                              'driving_mileage', 'night_driving_rate', 'start_up_rate'])
    # print(vehicle_summary_df)
    vehicle_summary_df = pd.concat([vehicle_summary_df, summary_df_by_vid], axis=0, sort=False)


start = time.time()
while loop:
    try:
        chunk = vehicle_data.get_chunk(chunkSize)
        chunk = clear_invalid_data(chunk)
        chunks.append(chunk)
    except StopIteration:
        loop = False
        print('Iteration is stopped.')

for vid in vid_list:
    print('generate ', vid)
    generate_summary_per_vid(vid)

vehicle_summary_df.to_csv('summary/summary.csv', encoding='utf-8')
time_elapsed = time.time() - start
print('The code run {:.0f}m {:.0f}s'.format(time_elapsed // 60, time_elapsed % 60))

