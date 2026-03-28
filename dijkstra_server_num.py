import pandas as pd
import numpy as np

class Dijkstra:
    def __init__(self, graph, start, goal):
        self.graph = graph      # 邻接表
        self.start = start      # 起点
        self.goal = goal        # 终点

        self.open_list = {}     # open 表
        self.closed_list = {}   # closed 表

        self.open_list[start] = 0.0     # 将起点放入 open_list 中

        self.parent = {start: None}     # 存储节点的父子关系。键为子节点，值为父节点。方便做最后路径的回溯
        self.min_dis = None             # 最短路径的长度

    def shortest_path(self):

        while True:
            if self.open_list is None:
                print('搜索失败， 结束！')
                break
            distance, min_node = min(zip(self.open_list.values(), self.open_list.keys()))      # 取出距离最小的节点
            self.open_list.pop(min_node)                                                       # 将其从 open_list 中去除

            self.closed_list[min_node] = distance                  # 将节点加入 closed_list 中

            if min_node == self.goal:                              # 如果节点为终点
                self.min_dis = distance
                shortest_path = [self.goal]                        # 记录从终点回溯的路径
                father_node = self.parent[self.goal]
                while father_node != self.start:
                    shortest_path.append(father_node)
                    father_node = self.parent[father_node]
                shortest_path.append(self.start)
                # print(shortest_path[::-1])                         # 逆序
                # print('最短路径的长度为：{}'.format(self.min_dis))
                # print('找到最短路径， 结束！')
                return shortest_path[::-1], self.min_dis			# 返回最短路径和最短路径长度

            for node in self.graph[min_node].keys():               # 遍历当前节点的邻接节点
                if node not in self.closed_list.keys():            # 邻接节点不在 closed_list 中
                    if node in self.open_list.keys():              # 如果节点在 open_list 中
                        if self.graph[min_node][node] + distance < self.open_list[node]:
                            self.open_list[node] = distance + self.graph[min_node][node]         # 更新节点的值
                            self.parent[node] = min_node           # 更新继承关系
                    else:                                          # 如果节点不在 open_list 中
                        self.open_list[node] = distance + self.graph[min_node][node]             # 计算节点的值，并加入 open_list 中
                        self.parent[node] = min_node               # 更新继承关系


if __name__ == '__main__':
    dis = pd.read_csv("data/dis_add.csv")
    station = pd.read_csv('./data/edge_cloud_add.csv')
    cloud = 1.5
    g = {}
    for i in range(len(dis)):
        load = {}
        for j in range(len(dis)):
            if dis.loc[i, str(j+1)]!=-1:
                load[str(j)] = dis.loc[i, str(j+1)]
        g[str(i)] = load
    dd = [[0 for _ in range(len(dis))] for _ in range(len(dis))]
    cc = [[0 for _ in range(len(dis))] for _ in range(len(dis))]
    for i in range(len(dis)):
        rou = pd.DataFrame(columns=[str(j) for j in range(len(dis))])
        for j in range(len(dis)):
            if j==i:
                rou.loc[0, str(j)] = i
                continue
            start = str(i)
            goal = str(j)
            dijk = Dijkstra(g, start, goal)
            l1, l2 = dijk.shortest_path()
            # 距离计算方案
            # l1 ['2', '78', '41', '1']
            # l2 2718.802600842121
            for k in range(len(l1)):
                rou.loc[k, str(j)] = l1[k]
            for m in range(len(l1)-1):
                a = int(l1[m])
                b = int(l1[m+1])
                af = station.loc[a, 'flag']
                bf = station.loc[a, 'flag']
                if af!=0 and bf!=0:
                    cc[i][j]+=cloud*dis.loc[a, str(b+1)]
                else:
                    cc[i][j]+=dis.loc[a, str(b+1)]
            dd[i][j] = l2
            print(i, j)
        rou.to_csv('./route/route_add_'+str(i)+'.csv', index=False)
    dd = pd.DataFrame(dd)
    dd.to_csv('./data/distance_add.csv', index=False)
    cc = pd.DataFrame(cc)
    cc.to_csv('./data/cost_rate_add.csv', index=False)

    # station.insert(loc=len(station.columns), column='cloud_id', value=np.NaN)
    cloud = station[station['flag']==2]
    for i in range(len(station)):
        flag = -1
        cloud_d = [dd.loc[i, len(station)-j-1] for j in range(len(cloud))]
        station.loc[i, 'cloud_id'] = len(cloud) - cloud_d.index(min(cloud_d)) - 1
    station.to_csv('./data/EUadd.csv', index=False)
