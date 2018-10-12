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
summary_driving_behavior = pd.DataFrame(columns=['T', 'T_a', 'T_d', 'T_c', 'T_i', 'v_max', 'v_m', 'a_max', 'a_a',
                                                 'a_min', 'a_d'])
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
    df['speed'] = df['speed'].apply(lambda row: float(row))
    df['mileage'] = df['mileage'].apply(lambda row: float(row))
    return df


def get_over_speed_rate(df, speed):
    origin_len = len(df)
    df = df.loc[df['speed'] > speed]
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


def generate_driving_behavior_df(dp_list):
    driving_behavior = {
        'T': 0,
        'T_a': 0,
        'T_d': 0,
        'T_c': 0,
        'T_i': 0,
        'v_max': 0,
        'v_m': 0,
        'a_max': 0,
        'a_a': 0,
        'a_min': 0,
        'a_d': 0
    }
    v_list = []
    acc_list = []
    dec_list = []
    for index in range(len(dp_list)):
        v_list.append(dp_list[index]['speed'])
        if index == (len(dp_list) - 1):
            driving_behavior['T'] = dp_list[index]['timestamp'] - dp_list[0]['timestamp']
        if index < (len(dp_list) - 1):
            if (dp_list[index + 1]['timestamp'] - dp_list[index]['timestamp']) * (1000.0/3600.0) == 0:
                continue
            a = (dp_list[index + 1]['speed'] - dp_list[index]['speed']) / \
                (dp_list[index + 1]['timestamp'] - dp_list[index]['timestamp']) * (1000.0/3600.0)
            if a >= 0.1:
                acc_list.append(a)
                driving_behavior['T_a'] += (dp_list[index + 1]['timestamp'] - dp_list[index]['timestamp'])
            if a <= -0.1:
                dec_list.append(a)
                driving_behavior['T_d'] += (dp_list[index + 1]['timestamp'] - dp_list[index]['timestamp'])
            if a == 0:
                driving_behavior['T_c'] += (dp_list[index + 1]['timestamp'] - dp_list[index]['timestamp'])
            if dp_list[index + 1]['speed'] == dp_list[index]['speed'] == 0:
                driving_behavior['T_i'] += (dp_list[index + 1]['timestamp'] - dp_list[index]['timestamp'])

    if len(v_list) > 0:
        driving_behavior['v_max'] = max(v_list)
        driving_behavior['v_m'] = sum(v_list) / len(v_list)
    if len(acc_list) > 0:
        driving_behavior['a_max'] = max(acc_list)
        driving_behavior['a_a'] = sum(acc_list) / len(acc_list)
    if len(dec_list) > 0:
        driving_behavior['a_min'] = min(dec_list)
        driving_behavior['a_d'] = sum(dec_list) / len(dec_list)
    vehicle_driving_behavior_df = pd.DataFrame([[driving_behavior['T'], driving_behavior['T_a'],
                                                 driving_behavior['T_d'],
                                                 driving_behavior['T_c'], driving_behavior['T_i'],
                                                 driving_behavior['v_max'],
                                                 driving_behavior['v_m'], driving_behavior['a_max'],
                                                 driving_behavior['a_a'],
                                                 driving_behavior['a_min'], driving_behavior['a_d']]],
                                               columns=['T', 'T_a', 'T_d', 'T_c', 'T_i', 'v_max', 'v_m',
                                                        'a_max', 'a_a', 'a_min', 'a_d'])
    return vehicle_driving_behavior_df


def generate_summary_driving_behavior_df(df):
    global summary_driving_behavior
    start_timestamp = 0
    # 一个驾驶行为片段
    driving_part = []
    # flag = True 时开始取下一个新的驾驶行为
    flag = True
    for index, row in df.iterrows():
        # 简化后一行数据
        simple_data = {
            'timestamp': 0,
            'speed': float(row['speed'])
        }
        timestamp = 0
        ts = time.strptime(str(row['daq_time']), '%Y%m%d%H%M%S')
        if flag:
            start_timestamp = float(time.mktime(ts))
            simple_data['timestamp'] = start_timestamp
            driving_part.append(simple_data)
        else:
            timestamp = float(time.mktime(ts))
            simple_data['timestamp'] = timestamp
            driving_part.append(simple_data)
        if timestamp-start_timestamp > 30:
            # 采样点不足则剔除
            if (timestamp-start_timestamp) / 30 > len(driving_part):
                flag = True
                driving_part = []
                continue
            else:
                summary_driving_behavior = pd.concat([summary_driving_behavior,
                                                      generate_driving_behavior_df(driving_part)], axis=0, sort=False)
                driving_part = []
        else:
            flag = False


def generate_summary_per_vid(vid):
    global chunk, driving_df, stalled_df, summary_driving_behavior
    # vid_type = vid[2]
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
    # 至少隔30s采样 取平均速度
    generate_summary_driving_behavior_df(driving_df)
    print(summary_driving_behavior)


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

summary_driving_behavior.to_csv('summary/summary.csv', encoding='utf-8')
time_elapsed = time.time() - start
print('The code run {:.0f}m {:.0f}s'.format(time_elapsed // 60, time_elapsed % 60))

