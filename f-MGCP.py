import pandas as pd
import numpy as np
from heapq import merge
import copy
import multiprocessing as mp
import random

enum_vec = [0, 1, 2, 3, 4]
# 读取连接
video_record = pd.read_csv('./FS/user.csv')
video = pd.read_csv('./FS/user_counts.csv')
video_num = len(video)
# video_num = 1
dis = pd.read_csv('./data/distance_add.csv')
user = pd.read_csv('./FS/venus.csv')

for kkk in enum_vec:
    node = pd.read_csv('./data/EU_cloud'+str(kkk)+'.csv')
    station_num = len(node)
    cloud_num = 6
    edge_num = station_num-cloud_num
    node_status = [node.loc[i, 'status'] for i in range(len(node))]

    station = [[node.loc[i, 'size']*1.0] for i in range(edge_num)]
    ss_o = [node.loc[i, 'size'] for i in range(edge_num)]
    station.extend([[node.loc[i+edge_num, 'size']*1.0] for i in range(cloud_num)])
    for i in range(len(station)):
        if node_status[i]==0: 
            station[i][0] = 0
            if i<edge_num:
                ss_o[i] = 0
    for i in range(len(station)):
        station[i].extend([0 for _ in range(video_num)])
    c_map = [-1 for _ in range(video_num)]
    for i in range(video_num):
        pre_cloud=[j for j in range(cloud_num)]
        random.shuffle(pre_cloud)
        for j in range(cloud_num):
            s_id = pre_cloud[j]+edge_num
            if station[s_id][0]>video.loc[i, 'size']:
                station[s_id][i+1]=1
                station[s_id][0]-=video.loc[i, 'size']
                c_map[i]=s_id
                break


    req = [[0 for _ in range(video_num)] for _ in range(edge_num)]
    for i in range(video_num):
        record = video_record[video_record['user_id']==video.loc[i, 'user_id']]
        a = record.index.to_list()
        for j in a:
            s_id = int(record.loc[j, 'station_id'])
            # print(s_id)
            req[s_id][i]+=1
    for i in range(edge_num):
        s = sum(req[i])
        for j in range(video_num):
            if s==0:
                req[i][j]==0
            else:
                req[i][j] = req[i][j]/s
    aud = []
    for i in range(edge_num):
        u = user[user['station_id']==float(i)]
        u_i = u.index.to_list()
        d = [u.loc[j, 'dis'] for j in u_i]
        if len(d)==0:
            aud.append(0)
        else:
            aud.append(sum(d)/len(d))

    def getMG(video_id, station_id, video_size):
        tbc = 0.5
        res = dis.loc[station_id, str(c_map[video_id])]/200000+0.5*1*req[station_id][video_id]*tbc*aud[station_id]+0.5*video_size/ss_o[station_id]
        res = res*video_size
        return res

    for i in range(edge_num):
        a = [[req[i][j], j] for j in range(video_num)]
        a = sorted(a, reverse=True)
        id_vec = []
        mg_vec = []
        size_vec = []
        for j in range(video_num):
            if a[j][0]==0:
                break
            v_id = a[j][1]
            v_size = video.loc[v_id, 'size']
            if station[i][0]>=v_size:
                id_vec.append(v_id)
                station[i][0]-=v_size
                mg_vec.append(getMG(v_id, i, v_size))
                size_vec.append(v_size)
            elif len(id_vec)!=0:
                mg = getMG(v_id, i, v_size)
                max_mg = max(mg_vec)
                max_id = mg_vec.index(max_mg)
                if mg<max_mg and station[i][0]+size_vec[max_id]>=v_size:
                    station[i][0] = station[i][0] - v_size + size_vec[max_id]
                    mg_vec.pop(max_id)
                    id_vec.pop(max_id)
                    size_vec.pop(max_id)
                    mg_vec.append(mg)
                    id_vec.append(v_id)
                    size_vec.append(v_size)
        for j in id_vec:
            station[i][j+1]=1

    rrr = pd.DataFrame(station)
    rrr.to_csv('./res_mp/result_MGCP_cnum'+str(kkk)+'.csv', index=False)