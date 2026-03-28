import pandas as pd
import numpy as np
from heapq import merge
import copy
import multiprocessing as mp
import time

enum_vec = [0, 1, 2, 3, 4]
# ssize_vec = [0.5]
kkk = enum_vec[0]
# # 读取连接
video_record = pd.read_csv('./FS/user.csv')
video = pd.read_csv('./FS/user_counts.csv')
video_num = len(video)
# video_num = 1
node = pd.read_csv('./data/EU_cloud'+str(kkk)+'.csv')
station_num = len(node)
cloud_num = 6
edge_num = station_num-cloud_num
user = pd.read_csv('./FS/venus.csv')
dis = pd.read_csv('./data/distance_add.csv')

dis=dis.iloc[:,:]
dis=dis.values
dis.tolist()

node_status = [node.loc[i, 'status'] for i in range(len(node))]

similarity = pd.read_csv('./FS/similarity.csv')
ux = [video.loc[i, 'size'] for i in range(len(video))]
ux = sorted(ux)
size = ux[int(len(ux)*0.25)]
count = video.loc[int(len(video)*0.25),'count']
vec = []
for i in range(len(video)):
    if video.loc[i, 'size']<size and video.loc[i, 'count']>count:
        vec.append(i)
sim_vec = copy.deepcopy(vec)
sim_index = [0 for _ in range(len(video))]
for i in range(len(sim_vec)):
    sim_index[sim_vec[i]] = i
combo = [[i] for i in range(len(video))]
combo_flag = [0 for _ in range(len(video))]

sim=similarity.iloc[:,:]
sim=sim.values
sim.tolist()
sim_vec = []
for i in range(len(sim)):
    for j in range(len(sim[i])):
        sim_vec.append(sim[i][j])
sim_vec = sorted(sim_vec, reverse=True)
sim_flag = 0.98

load_combo = []
for i in range(len(vec)):
    for j in range(len(vec)):
        if j<=i:
            continue
        elif similarity.loc[i, str(j)]>sim_flag:
            load_combo.append([i, j])
            combo_flag[vec[i]] = 1
            combo_flag[vec[j]] = 1
def j_sim(ax, index):
    for a in ax:
        if similarity.loc[index, str(a)]<=sim_flag:
            return False
    return True
while(len(load_combo)!=0):
    load_flag = [0 for _ in range(len(load_combo))]
    l = []
    for i in range(len(load_combo)):
        for j in range(len(vec)):
            if j<=max(load_combo[i]):
                continue
            elif len(load_combo[i])>2:
                break
            elif j_sim(load_combo[i], j):
                load_flag[i] = 1
                x = copy.deepcopy(load_combo[i])
                x.append(j)
                l.append(x)
    for i in range(len(load_combo)):
        combo.append([vec[load_combo[i][j]] for j in range(len(load_combo[i]))])
    # combo.extend(load_combo)
    combo_flag.extend(load_flag)
    load_combo = l

video_size = [0 for _ in range(len(combo))]
for i in range(len(combo)):
    for j in combo[i]:
        video_size[i]+=video.loc[j, 'size']

load_video = [0 for _ in range(len(combo))]
# load_station = [0 for _ in range(station_num)]
# 服务器j区域内的用户请求数据i，将会向服务器value请求
request_map = [[] for _ in range(video_num)]
# 服务器j区域内的用户向服务器k请求数据i，所产生的权重为value
value_map = [[[0 for _ in range(station_num)] for _ in range(edge_num)] for _ in range(video_num)]
station = [[node.loc[i, 'size']*1.0] for i in range(edge_num)]
station.extend([[node.loc[i+edge_num, 'size']*1.0] for i in range(cloud_num)])
for i in range(len(station)):
    station[i].extend([0 for _ in range(len(combo))])

vec_l = [[0 for _ in range(station_num)] for _ in range(video_num)]
vec = []
vec2 = []
# i存在于value
video_map = [[] for _ in range(len(combo))]
# value存在于i
video_map_n = [[] for _ in range(len(combo))]

def update(vec, station):
    v = 0
    for k in combo[vec[0][2]]:
        if station[vec[0][1]][k+1] == 1:
            v = 0
            break
        for j in range(edge_num):
            # if station[j][k+1]==1:
            #     continue
            if value_map[k][j][vec[0][1]]>value_map[k][j][request_map[k][j]]:
                v = v + value_map[k][j][vec[0][1]] - value_map[k][j][request_map[k][j]]
    if v>0:
        vec[0][0] = v
        vec[0][4] = load_video[vec[0][2]]
        vec_new = [vec[0]]
        vec.pop(0)
        global vec2
        vec2 = list(merge(vec2, vec_new, reverse=True))
        # vec = list(merge(vec, vec_new))
    else:
        vec.pop(0)

dec_flag = [1 for _ in range(len(combo))]
def decombo(combo_id):
    load = video_map[combo_id]
    combo_flag[combo_id] = 0
    i = 0
    for i in load:
        dec_flag[i] = 0
    # while i<len(vec):
    #     if vec[i][2] in load:
    #         vec.pop(i)
    #         i-=1
    #     i+=1
    # return

def get_value_map(i, j, v_size, v_record):
    r = []
    j = int(v_record.loc[j, 'venus_id'])
    d0 = user.loc[j, 'dis']
    n_station = int(user.loc[j, 'station_id'])
    for k in range(station_num):
        v=v_size*10000000/(d0+dis[n_station][k])
        if v==0:
            print(v)
        r.append([i, n_station, k, v])
    return r

T1 = time.time()
print(T1)
for i in range(video_num):
    # print(i, video_num)
    v_record = video_record[video_record['user_id']==video.loc[i, 'user_id']]
    v_size = video.loc[i, 'size']
    u_list = v_record.index.to_list()
    for j in u_list:
        a = get_value_map(i, j, v_size, v_record)
        for k in a:
            value_map[k[0]][k[1]][k[2]]= value_map[k[0]][k[1]][k[2]]+k[3]
T2 = time.time()
print(T2-T1)

def get_value_vec(i, j):
    all_size = sum([video.loc[k, 'size'] for k in combo[j]])
    if i<edge_num and all_size>180:
        return 0, 0, -1
    elif station[i][j+1]==1:
        return 0, 0, -1
    if j<video_num:
        v = 0
        for k in range(edge_num):
            if dis[k][request_map[j][k]]>dis[k][i]:
                v = v-value_map[j][k][request_map[j][k]]+value_map[j][k][i]
        vec_l[j][i] = v
    else:
        v = sum([vec_l[k][i] for k in combo[j]])
    # if v==0:
        # print(v)
    return i, j, v

# 初始化云
print('初始化云')
for i in range(video_num):
    a = []
    for j in range(cloud_num):
        cv = sum([value_map[i][k][j+edge_num] for k in range(edge_num)])
        a.append([cv, j])
    a.sort(key=lambda x: x[0], reverse=True)
    for j in range(cloud_num):
        pre_target=a[j][1]
        if station[pre_target+edge_num][0]>video_size[i] and node_status[pre_target+edge_num]==1:
            station[pre_target+edge_num][0]-=video_size[i]
            request_map[i] = [pre_target+edge_num for _ in range(edge_num)]
            station[pre_target+edge_num][i+1]=1
            break
T3 = time.time()
print(T3-T2)
# request_map_o = copy.deepcopy(request_map)

# 0: value
# 1: station_id
# 2: combo_id
print("vec构建")
for i in range(edge_num):
    if node_status[i]==0:
        continue
    for j in range(video_num):
        a = get_value_vec(i, j)
        if a[2]>0:
            vec.append([a[2], a[0], a[1], 0, 0])
T0 = time.time()
print(T0-T3)

for i in range(edge_num):
    for j in range(len(combo)-video_num):
        print(i, edge_num, len(combo), j)
        j = j+video_num
        a = get_value_vec(i, j)
        if a[2]>0:
            vec.append([a[2], a[0], a[1], 0, 0])
T4 = time.time()
print(T4-T0)
vec = sorted(vec, reverse=True)

print('video_map')
for i in range(len(combo)):
    for j in range(len(combo)):
        if i==j:
            continue
        flag = 0
        for k in combo[i]:
            if k not in combo[j]:
                flag = -1
                break
        if flag==0:
            video_map[i].append(j)
            video_map_n[j].append(i)
T5 = time.time()
print(T5-T4)

sim_min = [1 for _ in range(len(combo))]
for i in range(len(combo)):
    if i<video_num:
        continue
    for j in range(len(combo[i])):
        for k in range(len(combo[i])):
            if k<=j:
                continue
            if sim_min[i]>similarity.loc[sim_index[combo[i][j]], str(sim_index[combo[i][k]])]:
                sim_min[i] = similarity.loc[sim_index[combo[i][j]], str(sim_index[combo[i][k]])]

print('start')
print(len(vec))
print(vec[0][0])
min_size = min([video.loc[i, 'size'] for i in range(len(video))])
res_p = pd.DataFrame(columns=['i', 'len', 'place'])
for i in range(len(combo)):
    a = i
    res_p.loc[i, 'i'] = a
    res_p.loc[i, 'len'] = len(combo[i])
while(True):
    lv = len(vec)
    if lv==0:
        break
    # if lv%10000==0 and min_size>max(station[i][0] for i in range(edge_num)):
    #     break
    if station[vec[0][1]][vec[0][2]+1]==1 or dec_flag[vec[0][2]]==0 or node_status[vec[0][1]]==0:
        vec.pop(0)
        continue
    if vec[0][1]<edge_num and station[vec[0][1]][0]<video_size[vec[0][2]]:
        vec.pop(0)
        continue
    if vec[0][4]!=load_video[vec[0][2]]:
        update(vec, station)
        continue
    if combo_flag[vec[0][2]]==1:
        decombo(vec[0][2])
        continue
    if len(vec2)!=0 and (vec2[0][0]>vec[0][0] or len(vec2)>lv/100):
        vec = list(merge(vec, vec2, reverse=True))
        vec2 = []
        continue
    l_video = []
    res_p.loc[vec[0][2], 'place']+=1
    for i in combo[vec[0][2]]:
        if vec[0][1]<edge_num and station[vec[0][1]][i+1]!=1:
            station[vec[0][1]][0]-=video.loc[i, 'size']
            station[vec[0][1]][i+1] = 1
            res_p.loc[i, 'place']+=1
        for j in range(edge_num):
            if dis[j][request_map[i][j]]>dis[j][vec[0][1]]:
                request_map[i][j] = vec[0][1]
        l_video.extend(video_map[i])
        l_video.append(i)
    for j in video_map_n[vec[0][2]]:
        station[vec[0][1]][j+1] = 1
    station[vec[0][1]][vec[0][2]] = 1
    for i in list(set(l_video)):
        load_video[i]+=1
    if vec[0][2]>=video_num and combo_flag[vec[0][2]]>0.99:
        dec_flag[vec[0][2]]=0
    elif vec[0][2]>=video_num and combo_flag[vec[0][2]]<1:
        if sim_min[vec[0][2]]>0.999:
            combo_flag[vec[0][2]]+=0.4
        elif sim_min[vec[0][2]]>0.995:
            combo_flag[vec[0][2]]+=1.1
        else:
            combo_flag[vec[0][2]]+=1.1
    vec.pop(0)
    print(lv)
rrr = pd.DataFrame(station)
rrr.to_csv('./res_mp/result_cnum'+str(kkk)+'.csv', index=False)
res_p.to_csv('res_p_cnum'+str(kkk)+'.csv', index=False)
T6 = time.time()
print(T6-T5)