import pandas as pd
import numpy as np
from sklearn.neighbors import KernelDensity
from scipy.stats import entropy
import copy
import multiprocessing as mp
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial import distance



vvv = ['', '_2000', '_2500', '_3000', '_3500', '_4000', '_4500', '_5000']
# vvv = ['_d0', '_d12.5', '_d25', '_d37.5', '_d50']

dis = pd.read_csv('./data/distance.csv')
dis=dis.iloc[:,:]
dis=dis.values
dis.tolist()

for kkk in range(len(vvv)):
    user = pd.read_csv('./FS/user'+vvv[kkk]+'.csv')
    # user = pd.read_csv('./FS/user.csv')
    venus = pd.read_csv('./FS/venus.csv')
    video = pd.read_csv('./FS/user_counts'+vvv[kkk]+'.csv')
    station = pd.read_csv('./data/EU455.csv')

    # dis_edge = pd.read_csv()
    ux = [video.loc[i, 'size'] for i in range(len(video))]
    ux = sorted(ux)
    size = ux[int(len(ux)*0.25)]
    count = video.loc[int(len(video)*0.25),'count']
    # 服务器最大容量+
    siZe = video['size'].max()

    maxx = station['x'].max()
    minx = station['x'].min()
    maxy = station['y'].max()
    miny = station['y'].min()
    cloud_num = 4

    vec = []
    for i in range(len(video)):
        if video.loc[i, 'size']<size and video.loc[i, 'count']>count:
            vec.append(i)

    weight = [[0 for _ in range(len(station)-cloud_num)] for _ in range(len(vec))]
    for i in range(len(vec)):
        print(i, len(vec))
        video_id = video.loc[vec[i], 'user_id']
        uu = copy.deepcopy(user[user['user_id']==video_id])
        uu.reset_index(inplace=True)
        for j in range(len(uu)):
            venus_id = int(uu.loc[j, 'venus_id'])
            station_id = int(venus.loc[venus_id, 'station_id'])
            # weight[i][station_id]+=1000/venus.loc[venus_id, 'dis']
            for k in range(len(station)-cloud_num):
                weight[i][k]+=1000/(venus.loc[venus_id, 'dis']+dis[station_id][k])
    # for i in range(len(weight)):
    #     w_max = max(weight[i])
    #     w_min = min(weight[i])
    #     for j in range(len(weight[i])):
    #         weight[i][j] = (weight[i][j]-w_min)/(w_max-w_min)

    res = [[0 for _ in range(len(vec))] for _ in range(len(vec))]
    for i in range(len(vec)):
        for j in range(len(vec)):
            print(i, j)
            if i<j:
                # wa = np.array(weight[i]).reshape(-1, 1)
                # wb = np.array(weight[j]).reshape(-1, 1)
                coss = 1-distance.cosine(np.array(weight[i]), np.array(weight[j]))
                res[i][j] = coss
            elif i==j:
                res[i][j] = 0
            else:
                res[i][j] = res[j][i]

    res = pd.DataFrame(res)
    res.to_csv('./FS/similarity'+vvv[kkk]+'.csv', index=False)