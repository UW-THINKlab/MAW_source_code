"""
    incremental clustering developed in 2018 TRC paper
    together with k-means to correct an order issue related to incremental clustering
:param user:
:param spat_constr:
:param dur_constr:
:return: modified user traces
"""


import sys
import numpy as np


from distance import distance
from class_cluster import cluster
# from kmeans_clustering import K_meansCluster



def K_meansCluster(L):
    """
            Kmeans clustering
    :param L: a list of clusters of traces
    :return: a list of clusters
    """

    uniqMonthGPSList = []
    for c in L:
        uniqMonthGPSList.extend(c.pList)

    Kcluster = [c.pList for c in L]
    while True:
        KcenterList = [(np.mean([p[0] for p in c]), np.mean([p[1] for p in c])) for c in Kcluster]
        Kcluster = [[] for _ in range(len(Kcluster))]

        for point in uniqMonthGPSList:
            closestCluIndex = -1
            closestDist2Clu = 1000000
            for i in range(len(KcenterList)):
                distP2C = distance(KcenterList[i][0], KcenterList[i][1], point[0], point[1])
                if closestDist2Clu > distP2C:
                    closestDist2Clu = distP2C
                    closestCluIndex = i
            Kcluster[closestCluIndex].append(point)

        i = 0
        while i < len(Kcluster):
            if len(Kcluster[i]) == 0:
                del Kcluster[i]
            else:
                i += 1

        FlagChanged = False
        for i in range(len(Kcluster)):
            cent = (np.mean([p[0] for p in Kcluster[i]]), np.mean([p[1] for p in Kcluster[i]]))
            if cent != KcenterList[i]:
                FlagChanged = True
                break

        if FlagChanged == False:
            break

    return L


def cluster_incremental(user, spat_constr, dur_constr=None):
    # spat_constr #200.0/1000 #0.2Km
    # dur_constr # 0 or 300second

    if dur_constr:  # cluster locations of stays to obtain aggregated stayes
        loc4cluster = list(set([(trace[6], trace[7]) for d in user for trace in user[d] if int(trace[9]) >= dur_constr]))
    else:  # cluster original locations to obtain stays
        loc4cluster = list(set([(trace[3], trace[4]) for d in user for trace in user[d]]))

    if len(loc4cluster) == 0:
        return (user)

    ## start clustering
    L = []
    Cnew = cluster()
    Cnew.addPoint(loc4cluster[0])
    L.append(Cnew)
    Ccurrent = Cnew
    for i in range(1, len(loc4cluster)):
        if Ccurrent.distance_C_point(loc4cluster[i]) < spat_constr:
            Ccurrent.addPoint(loc4cluster[i])
        else:
            Ccurrent = None
            for C in L:
                if C.distance_C_point(loc4cluster[i]) < spat_constr:
                    C.addPoint(loc4cluster[i])
                    Ccurrent = C
                    break
            if Ccurrent == None:
                Cnew = cluster()
                Cnew.addPoint(loc4cluster[i])
                L.append(Cnew)
                Ccurrent = Cnew

    L = K_meansCluster(L) # correct an order issue related to incremental clustering

    ## centers of each locations that are clustered
    mapLocation2cluCenter = {}
    for c in L:
        r = int(1000*c.radiusC()) #
        cent = [str(np.mean([p[0] for p in c.pList])), str(np.mean([p[1] for p in c.pList]))]
        for p in c.pList:
            mapLocation2cluCenter[(str(p[0]),str(p[1]))] = (cent[0], cent[1], r)

    if dur_constr:  # modify locations of stays to aggregated centers of stays
        for d in user.keys():
            for trace in user[d]:
                if (trace[6], trace[7]) in mapLocation2cluCenter:
                    trace[6], trace[7], trace[8] = mapLocation2cluCenter[(trace[6], trace[7])][0], \
                                                   mapLocation2cluCenter[(trace[6], trace[7])][1], \
                                                   max(mapLocation2cluCenter[(trace[6], trace[7])][2], int(trace[8]))
    else:  # record stay locations of original locations
        for d in user.keys():
            for trace in user[d]:
                if (trace[3], trace[4]) in mapLocation2cluCenter:
                    trace[6], trace[7], trace[8] = mapLocation2cluCenter[(trace[3], trace[4])][0], \
                                                   mapLocation2cluCenter[(trace[3], trace[4])][1], \
                                                   max(mapLocation2cluCenter[(trace[3], trace[4])][2], int(trace[5]))

    return (user)