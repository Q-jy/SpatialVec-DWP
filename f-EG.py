import pandas as pd
import numpy as np
from heapq import merge
import copy
import multiprocessing as mp
import hashlib
import math
import random


enum_vec = [0, 1, 2, 3, 4]
# 读取连接
video_record = pd.read_csv('./FS/user.csv')
video = pd.read_csv('./FS/user_counts.csv')
video_num = len(video)
# video_num = 1
dis = pd.read_csv('./data/distance_add.csv')
user = pd.read_csv('./FS/venus.csv')

dis=dis.iloc[:,:]
dis=dis.values
dis.tolist()

for kkk in enum_vec:
    node = pd.read_csv('./data/EU_cloud'+str(kkk)+'.csv')
    station_num = len(node)
    cloud_num = 6
    edge_num = station_num-cloud_num

    omg = 1/sum([1/(i+1) for i in range(video_num)])
    pop_vec = [[0 for _ in range(video_num)] for _ in range(edge_num)]
    for i in range(video_num):
        v_record = video_record[video_record['user_id']==video.loc[i, 'user_id']]
        ve_record = v_record['station_id']
        ve_record = ve_record.loc[:].values
        for j in ve_record:
            pop_vec[int(j)][i]+=1
    for i in range(edge_num):
        zipped = sorted([[pop_vec[i][j], j] for j in range(video_num)], reverse=True)
        for j in range(len(zipped)):
            pop_vec[i][zipped[j][1]] = omg/(j+1)

    station = [[node.loc[i, 'size']*1.0] for i in range(edge_num)]
    station.extend([[node.loc[i+edge_num, 'size']*1.0] for i in range(cloud_num)])
    node_status = [node.loc[i, 'status'] for i in range(len(node))]
    for i in range(len(station)):
        if node_status[i]==0: 
            station[i][0] = 0
    for i in range(len(station)):
        station[i].extend([0 for _ in range(video_num)])

    video_size = [video.loc[i, 'size'] for i in range(video_num)]
    station_size = [station[i][0] for i in range(station_num)]

    cloud_lat = [[200 for _ in range(video_num)] for _ in range(edge_num)]
    for i in range(video_num):
        pre_cloud=[j for j in range(cloud_num)]
        random.shuffle(pre_cloud)
        for j in range(cloud_num):
            index = pre_cloud[j]+edge_num
            if station[index][0]>=video_size[i] and node_status[index]==1:
                station[index][i+1] = 1
                station[index][0]-=video_size[i]
                for k in range(edge_num):
                    cloud_lat[k][i] = video_size[i]/200+dis[k][index]/200000
                break

    change_num = -1
    sum_place = 0
    i_num = 0
    while(change_num==-1 or (sum_place!=0 and change_num/sum_place>=0.05 and i_num<10)):
        change_num = 0
        i_num+=1
        for i in range(edge_num):
            if node_status[i]==0:
                continue
            pop = []
            for jj in range(video_num):
                pre_dis = -1
                for j in range(edge_num):
                    if station[j][jj+1]==1:
                        if pre_dis == -1:
                            pre_dis = dis[j][i]
                        elif dis[j][i]<pre_dis:
                            pre_dis = dis[j][i]
                if pre_dis == -1:
                    fa = (pre_dis/200000+video_size[jj]/200)/cloud_lat[i][jj]
                else:
                    fa = 1
                pop.append(pop_vec[i][jj]*fa)
            zipped = zip(pop, [j for j in range(video_num)])
            zipped = sorted(zipped, reverse=True)
            ssize = station_size[i]
            for j in range(video_num):
                index = zipped[j][1]
                if ssize>video_size[index]:
                    ssize-=video_size[index]
                    if station[i][j+1]!=1:
                        change_num+=1
                        station[i][j+1] = 1
                else:
                    station[i][j+1] = 0
        sum_place = sum([sum(station[kk])-station[kk][0] for kk in range(edge_num)])
        print(change_num, sum_place)
    rrr = pd.DataFrame(station)
    rrr.to_csv('./res_mp/result_EG_cnum'+str(kkk)+'.csv', index=False)