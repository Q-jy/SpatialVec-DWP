import pandas as pd
import numpy as np
from heapq import merge
import copy
import multiprocessing as mp

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
    station.extend([[node.loc[i+edge_num, 'size']*1.0] for i in range(cloud_num)])
    for i in range(len(station)):
        if node_status[i]==0: 
            station[i][0] = 0
    for i in range(len(station)):
        station[i].extend([0 for _ in range(video_num)])

    def getSite(video_id, flag, max_site, choice, map_site, site_user):
        if flag==max_site:
            return
        value = 0
        s_id = 0
        video_size = video.loc[video_id, 'size']
        for i in range(edge_num):
            v = 0
            if i in choice:
                continue
            if i<edge_num and station[i][0]<video_size:
                continue
            for j in range(len(site_user)):
                if site_user[j]!=0 and dis.loc[j, str(map_site[j])]>dis.loc[j, str(i)]:
                    v = v + (dis.loc[j, str(map_site[j])] - dis.loc[j, str(i)])*site_user[j]
            if v>value:
                value = v
                s_id = i
        if value==0:
            return
        else:
            choice.append(s_id)
            for j in range(len(map_site)):
                if dis.loc[j, str(map_site[j])]>dis.loc[j, str(s_id)]:
                    map_site[j] = s_id
            station[s_id][video_id+1] = 1
            if s_id<edge_num:
                station[s_id][0]-=video_size
            getSite(video_id, flag+1, max_site, choice, map_site, site_user)
        return


    for i in range(video_num):
        record = video_record[video_record['user_id']==video.loc[i, 'user_id']]
        max_site = len(record['station_id'].value_counts())
        b = record['station_id'].value_counts()
        a = b.index.to_list()
        site_user = []
        for j in range(edge_num):
            if float(j) in a:
                site_user.append(int(b.loc[float(j)]))
            else:
                site_user.append(0)
        # cloud
        value = -1
        c_id = -1
        for j in range(cloud_num):
            v = 0
            for k in range(edge_num):
                v+=dis.loc[k, str(edge_num+j)] * site_user[k]
            if (value==-1 or value<v) and station[j+edge_num][0]>=video.loc[i, 'size']:
                value = v
                c_id = j
        if value!=-1:
            map_site = [c_id+edge_num for _ in range(edge_num)]
            station[c_id+edge_num][i+1] = 1
            station[c_id+edge_num][0]-=video.loc[i, 'size']
        # edge
        getSite(i, 1, max_site, [c_id+edge_num], map_site, site_user)
    rrr = pd.DataFrame(station)
    rrr.to_csv('./res_mp/result_iFogDP_cnum'+str(kkk)+'.csv', index=False)