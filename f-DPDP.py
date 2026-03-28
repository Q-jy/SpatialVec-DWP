import pandas as pd
import numpy as np
from heapq import merge
import copy
import multiprocessing as mp
import hashlib
import math

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

mdis = copy.deepcopy(dis)
for i in range(len(dis)):
    for j in range(len(dis)):
        mdis[i][j] = dis[i][j]*dis[i][j]

for kkk in enum_vec:

    node = pd.read_csv('./data/EU_cloud'+str(kkk)+'.csv')
    station_num = len(node)
    cloud_num = 6
    edge_num = station_num-cloud_num
    node_status = [node.loc[i, 'status'] for i in range(len(node))]

    mJ = 1-1/station_num*np.ones([station_num, station_num])
    mdis = np.array(mdis)
    mB = 0.5*mJ@mdis@mJ
    print(mB)

    eig, feav = np.linalg.eig(mB)
    e = []
    for i in range(len(eig)):
        e.append([eig[i], i])
    e = sorted(e, reverse=True)
    print(e[0][1], e[1][1])
    a1 = e[0][1]
    a2 = e[1][1]
    e2 = np.array([feav[a1], feav[a2]])
    a2 = np.array([[e[0][0], 0],
                [0, e[1][0]]])

    v, P = np.linalg.eig(a2)
    V = np.diag(v**(0.5))
    transfer_mat = P @ V @ np.linalg.inv(P)
    mQ = np.transpose(e2)@transfer_mat

    mQmax = abs(np.max(mQ))
    mQmin = abs(np.min(mQ))
    if mQmax>mQmin:
        qmax = mQmax
    else:
        qmax = mQmin
    for i in range(len(mQ)):
        mQ[i][0] = mQ[i][0]/pow(2, 0.5)/qmax
        mQ[i][1] = mQ[i][1]/pow(2, 0.5)/qmax

    station = [[node.loc[i, 'size']*1.0] for i in range(edge_num)]
    station.extend([[node.loc[i+edge_num, 'size']*1.0] for i in range(cloud_num)])
    for i in range(len(station)):
        if node_status[i]==0: 
            station[i][0] = 0
    for i in range(len(station)):
        station[i].extend([0 for _ in range(video_num)])

    req_max = video.loc[0, 'count']
    size_max = video['size'].max()
    cap_sum = sum([station[i][0] for i in range(edge_num)])
    num_vec = []
    now_vec = [0 for _ in range(video_num)]
    obj = hashlib.sha256()
    for i in range(video_num):
        obj.update(str(video.loc[i]['user_id']).encode('utf-8'))
        t = obj.hexdigest()[-4:]
        t = int(t, 16)
        h_id = t/(4294967295.0)

        video_size = video.loc[i, 'size']
        req_num = video.loc[i, 'count']
        cap_now = sum([station[i][0] for i in range(edge_num)])
        num = (1/3*video_size/size_max+1/3*req_num/req_max+1/3*cap_now/cap_sum)*0.03*518
        num = int(num)
        num+=1
        num_vec.append(num)
        for j in range(num):
            if j>0 and 1-req_num/req_max<0.01:
                r = 1.5-req_num/req_max
            else:
                r = 1-req_num/req_max
            d = 2*math.pi*(h_id+(j+1)/num)
            x = r*math.cos(d)
            y = r*math.sin(d)
            dmin = -1
            index = -1
            if j==0:
                for k in range(cloud_num):
                    if station[k+edge_num][0]<video_size:
                        continue
                    d = (mQ[k+edge_num][0]-x)*(mQ[k+edge_num][0]-x)+(mQ[k+edge_num][1]-y)*(mQ[k+edge_num][1]-y)
                    if dmin==-1 or dmin>d:
                        dmin = d
                        index = k
                if dmin!=-1:
                    station[index+edge_num][i+1] = 1
                    now_vec[i]+=1
                    station[index+edge_num][0]-=video_size
                    continue
            for k in range(edge_num):
                if station[k][i+1] == 1 or station[k][0]<video_size:
                    continue
                d = (mQ[k][0]-x)*(mQ[k][0]-x)+(mQ[k][1]-y)*(mQ[k][1]-y)
                if dmin==-1 or dmin>d:
                    dmin = d
                    index = k
            if index==-1:
                break
            station[index][i+1] = 1
            station[index][0]-=video_size
            now_vec[i]+=1
    rrr = pd.DataFrame(station)
    rrr.to_csv('./res_mp/result_DPDP_cnum'+str(kkk)+'.csv', index=False)
    print(num_vec)
    print(num_vec[0])
    print(now_vec)