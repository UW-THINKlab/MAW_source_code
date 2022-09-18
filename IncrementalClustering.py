'''
Performe clustering on multiple locations of one user based on a spatial threshold
Each cluster of locations represents a potential stay 

input:
    gps stay information / celluar stay information
    spatial threshold
    duration constraint threshold (for detect common stay)
outout:
    potential stays represented by clusters of locations
'''

import sys, json,os, psutil, csv, time
import numpy as np
from distance import distance
from class_cluster import cluster
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import current_process, Lock, cpu_count

import geopy
from sklearn.cluster import KMeans

def init(l):
    global lock
    lock = l

def K_meansClusterLloyd(L):
    uniqMonthGPSList = []
    for c in L:
        uniqMonthGPSList.extend(c.pList)
        
    Kcluster = [c.pList for c in L]
    k = len(Kcluster)
    
    ##project coordinates on to plane
    ##search "python lat long onto plane": https://pypi.org/project/stateplane/
    ##search "python project lat long to x y": https://gis.stackexchange.com/questions/212723/how-can-i-convert-lon-lat-coordinates-to-x-y
    ###The above methods not used
    y_center = np.mean([p[0] for p in uniqMonthGPSList])
    x_center = np.mean([p[1] for p in uniqMonthGPSList])
   
    distance_coord = np.empty((0, 2))
    for p in uniqMonthGPSList:
        x_distance = distance(y_center,x_center,y_center,p[1])
        y_distance = distance(y_center,x_center,p[0],x_center)
        if p[0] < y_center:
            y_distance = - y_distance
        if p[1] < x_center:
            x_distance = - x_distance
        distance_coord = np.append(distance_coord, np.array([[y_distance,x_distance]]), axis=0)
        
    initial_centers = np.empty((0, 2))
    i=0
    for c in L:
        num_point = len(c.pList)
        points = distance_coord[i:(i+num_point)]
        ctr = np.mean(points,axis=0,keepdims=True)
        initial_centers = np.append(initial_centers, ctr, axis=0)
        i=i+num_point
    
    kmeans = KMeans(n_clusters=k,init=initial_centers).fit(distance_coord)
    lab = kmeans.labels_
    membership = {clus:[] for clus in set(lab)}
    for j in range(0,len(uniqMonthGPSList)):
        membership[lab[j]].append(uniqMonthGPSList[j])
        
    L_new = []
    for a_cluster in membership:
        newC = cluster()
        for a_location in membership[a_cluster]:
            newC.addPoint(a_location)
        L_new.append(newC)
        
    return L_new


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

    L = K_meansClusterLloyd(L) # correct an order issue related to incremental clustering

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


def func(args):

    name, user, spatial_constraint, dur_constraint, outputFile = args

    if dur_constraint == -1:
        user = cluster_incremental(user, spatial_constraint)

    else:
        user = cluster_incremental(user, spatial_constraint, dur_constraint)
        

    with lock:
        f = open(outputFile, 'a')
        writeCSV = csv.writer(f, delimiter=',')

        for day in sorted(user.keys()):
            for trace in user[day]:
                trace[1] = name
                writeCSV.writerow(trace)
        f.close()


if __name__ == '__main__':
    '''
    param: 
        inputFile
        partitionThreshold
    '''
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    spatial_constraint = float(sys.argv[3])
    dur_constraint = int(sys.argv[4])

    # tmp file
    outputFile = outputFile.replace('.csv','_tmp.csv')

    f = open(outputFile, 'w')
    f.write('unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n')
    f.close()


    l = Lock() # thread locker
    pool = Pool(cpu_count(), initializer=init, initargs=(l,))

    # fixed param
    user_num_in_mem = 1000

    usernamelist = set() # user names
    with open(inputFile,'rU') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            #if not len(row) ==12 : continue
            usernamelist.add(row[1])  # get ID list; the second colume is userID
    usernamelist = list(usernamelist)

    print('total number of users to be processed: ', len(usernamelist))

    def divide_chunks(usernamelist, n):
        for i in range(0, len(usernamelist), n): # looping till length usernamelist
            yield usernamelist[i:i + n]

    usernamechunks = list(divide_chunks(usernamelist, user_num_in_mem))

    print('number of chunks to be processed', len(usernamechunks))

    ## read and process traces for one bulk
    while (len(usernamechunks)):
        namechunk = usernamechunks.pop()
        print("Start processing bulk: ", len(usernamechunks) + 1, ' at time: ', time.strftime("%m%d-%H:%M"), ' memory: ', psutil.virtual_memory().percent)

        UserList = {name: defaultdict(list) for name in namechunk}

        with open(inputFile,'rU') as readfile:
            readCSV = csv.reader(readfile, delimiter=',')
            readCSV.next()
            for row in readCSV:
                #if not len(row) ==12 : continue
                #if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                #if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2): continue
                #if row[6] == '-' or row[7] == '-': continue 
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        print("End reading")

        # pool 
        tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], spatial_constraint, dur_constraint, outputFile) for name in UserList]]

        finishit = [t.get() for t in tasks]
        '''
        for name in UserList:
            func((name, UserList[name], spatial_constraint, dur_constraint, outputFile))
        '''       
    pool.close()
    pool.join()


    outputFile_real = outputFile.replace('_tmp.csv','.csv')
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile,outputFile_real)


