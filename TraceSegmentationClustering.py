
import numpy as np
from itertools import combinations
from distance import distance

import sys, json,os, psutil, csv, time, func_timeout
import numpy as np
from distance import distance
from class_cluster import cluster
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import current_process, Lock, cpu_count

def init(l):
    global lock
    lock = l

def diameterExceedCnstr(traj, i, j, spat_constr):
    """
    for function cluster_traceSegmentation (defined below) use only
    purpose: check the greatest distance between any two locations with in the set traj[i:j]
            and compare the distance with constraint
    remember, computing distance() is costly and this why this function seems so complicated.
    :param traj:
    :param i:
    :param j:
    :param spat_constr:
    :return: Ture or False
    """
    loc = list(set([(round(float(traj[m][3]),5),round(float(traj[m][4]),5))  for m in range(i,j+1)]))# unique locations
    if len(loc) <= 1:
        return False
    if distance(traj[i][3],traj[i][4],traj[j][3],traj[j][4])>spat_constr: # check the first and last trace
        return True
    else:
        # guess the max distance pair; approximate distance
        pairloc = list(combinations(loc, 2))
        max_i = 0
        max_d = 0
        for i in range(len(pairloc)):
            appx_d = abs(pairloc[i][0][0] - pairloc[i][1][0]) \
                     + abs(pairloc[i][0][1] - pairloc[i][1][1])
            if appx_d > max_d:
                max_d = appx_d
                max_i = i
        if distance(pairloc[max_i][0][0], pairloc[max_i][0][1], pairloc[max_i][1][0],
                    pairloc[max_i][1][1]) > spat_constr:
            return True
        else:
            #try to reduce the size of pairloc
            max_ln_lat = (abs(pairloc[max_i][0][0] - pairloc[max_i][1][0]),
                          abs(pairloc[max_i][0][1] - pairloc[max_i][1][1]))
            m = 0
            while m < len(pairloc):
                if abs(pairloc[m][0][0] - pairloc[m][1][0]) < max_ln_lat[0] \
                        and abs(pairloc[m][0][1] - pairloc[m][1][1]) < max_ln_lat[1]:
                    del pairloc[m]
                else:
                    m += 1
            diam_list = [distance(pair[0][0], pair[0][1], pair[1][0], pair[1][1]) for pair in pairloc]
            if max(diam_list) > spat_constr:
                return True
            else:
                return False

def diameterExceedCnstr_newTrace(trace_set, new_trace, spat_constr):
    max_x = 0
    max_y = 0
    max_aprox = 0
    for an_existing_trace in trace_set:
        approx_dist = (an_existing_trace[0] - new_trace[0])**2 + (an_existing_trace[1] - new_trace[1])**2
        if approx_dist > max_aprox:
            max_aprox = approx_dist
            max_x = an_existing_trace[0]
            max_y = an_existing_trace[1]

    if distance(max_x, max_y, new_trace[0], new_trace[1]) > spat_constr:
        return True

    for an_existing_trace in trace_set:
        if (an_existing_trace[0] - new_trace[0])**2 < (max_x - new_trace[0])**2 \
            and (an_existing_trace[1] - new_trace[1])**2 < (max_y - new_trace[1])**2:
            continue
        dist = distance(an_existing_trace[0],an_existing_trace[1], new_trace[0], new_trace[1])
        if dist > spat_constr:
            return True

    return False



def cluster_traceSegmentation(user, spat_constr, dur_constr):
    for day in user.keys():
        traj = user[day]
        i = 0
        while (i<len(traj)-1):
            j = i
            flag = False
            while (int(traj[j][0])-int(traj[i][0])<dur_constr):#j=min k s.t. traj_k - traj_i >= dur
                j+=1
                if (j==len(traj)):
                    flag = True
                    break
            if flag:
                break
            if diameterExceedCnstr(traj,i,j,spat_constr):
                i += 1
            else:
                j_prime = j
                gps_set = set([(round(float(traj[m][3]),5),round(float(traj[m][4]),5)) for m in range(i,j+1)])
                for k in range(j_prime+1, len(traj),1): # #j: max k subject to Diameter(R,i,k)<=spat_constraint
                    if (round(float(traj[k][3]), 5), round(float(traj[k][4]), 5)) in gps_set:
                        j = k
                    elif not diameterExceedCnstr_newTrace(gps_set, (round(float(traj[k][3]), 5), round(float(traj[k][4]), 5)), spat_constr):#diameterExceedCnstr(traj,i,k, spat_constr):
                        j = k
                        gps_set.add((round(float(traj[k][3]), 5), round(float(traj[k][4]), 5)))
                    else:
                        break
                mean_lat, mean_long = str(np.mean([float(traj[k][3]) for k in range(i,j+1)])), \
                                      str(np.mean([float(traj[k][4]) for k in range(i,j+1)]))
                dur = str(int(traj[j][0]) - int(traj[i][0]))  # give duration
                for k in range(i, j + 1):  # give cluster center
                    traj[k][6], traj[k][7], traj[k][9] = mean_lat, mean_long, dur
                i = j+1
        user[day] = traj
    
    ## Recombine stays that (1) don't have transit points between them and (2) are within the distance threshold.
    stays_combined = []
    all_stays = []
    
    day_set = list(user.keys())
    day_set.sort()
    
    for a_day in day_set:
        for a_location in user[a_day]:
            if len(all_stays) == 0:
                all_stays.append([a_location])
            else:
                last_stay = (all_stays[-1][-1][6], all_stays[-1][-1][7])
                if a_location[6] == last_stay[0] and a_location[7] == last_stay[1]:
                    all_stays[-1].append(a_location)
                else:
                    all_stays.append([a_location])
                    
    stay_index = 0
    stays_combined.append(all_stays[0])
    all_stays.pop(0)
    
    update_lat = float(stays_combined[-1][-1][6])
    update_long = float(stays_combined[-1][-1][7])
    
    while len(all_stays) > 0:
        current_stay = all_stays.pop(0)
        if tuple(stays_combined[-1][-1][6:8]) == ('-1','-1'):
            stays_combined.append(current_stay)
            update_lat = float(stays_combined[-1][-1][6])
            update_long = float(stays_combined[-1][-1][7])
        else:
            if tuple(current_stay[-1][6:8]) != ('-1','-1'):
                if distance(float(current_stay[-1][6]), float(current_stay[-1][7]), update_lat, update_long) < 0.2:
                    stays_combined[-1].extend(current_stay)
                    lat_set = set([float(x[6]) for x in stays_combined[-1]])
                    long_set = set([float(x[7]) for x in stays_combined[-1]])
                    update_lat = np.mean(list(lat_set))
                    update_long = np.mean(list(long_set))
                else:
                    stays_combined.append(current_stay)
                    update_lat = float(stays_combined[-1][-1][6])
                    update_long = float(stays_combined[-1][-1][7])
            else:
                if len(all_stays) == 0:
                    stays_combined.append(current_stay)
                else:
                    next_stay = all_stays.pop(0)
                    if distance(float(next_stay[-1][6]), float(next_stay[-1][7]), update_lat,update_long) < 0.2:
                        stays_combined[-1].extend(current_stay)
                        stays_combined[-1].extend(next_stay)
                        lat_set = set([float(x[6]) for x in stays_combined[-1]])
                        long_set = set([float(x[7]) for x in stays_combined[-1]])
                        lat_set = [x for x in lat_set if x != -1.0]
                        long_set = [x for x in long_set if x != -1.0]
                        update_lat = np.mean(lat_set)
                        update_long = np.mean(long_set)
                    else:
                        stays_combined.append(current_stay)
                        stays_combined.append(next_stay)
                        update_lat = float(stays_combined[-1][-1][6])
                        update_long = float(stays_combined[-1][-1][7])
    
    stays_output = []
    for a_stay in stays_combined:
        lat_set = set([float(x[6]) for x in a_stay])
        long_set = set([float(x[7]) for x in a_stay])
        lat_set = [x for x in lat_set if x != -1.0]
        long_set = [x for x in long_set if x != -1.0]
        if len(lat_set) > 0 and len(long_set) > 0:
            new_lat = np.mean(lat_set)
            new_long = np.mean(long_set)
        else:
            new_lat = -1
            new_long = -1
        for i in range(0, len(a_stay)):
            a_stay[i][6] = str(new_lat)
            a_stay[i][7] = str(new_long)
        stays_output.append(a_stay)
        
    ##Convert stays into a disctionary
    dict_output = {}
    for a_stay in stays_output:
        for a_record in a_stay:
            start_date = a_record[-1][0:6]
            if start_date in dict_output:
                dict_output[start_date].append(a_record)
            else:
                dict_output[start_date] = [a_record]
        
    return (dict_output)

def func(args):
    name, user, spatial_constraint_gps, dur_constraint, outputFile = args
    try:
        user = cluster_traceSegmentation(user,spatial_constraint_gps, dur_constraint)
        with lock:
            f = open(outputFile, 'a')
            writeCSV = csv.writer(f, delimiter=',')

            for day in sorted(user.keys()):
                for trace in user[day]:
                    trace[1] = name
                    writeCSV.writerow(trace)
            f.close()
    except:
        print('overtime!')
        return

if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    spatial_constraint_gps = float(sys.argv[3])
    duration_constraint = int(sys.argv[4])

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
        readCSV.next()
        for row in readCSV:
            if not len(row) ==12 : continue
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
                if not len(row) ==12 or len(row[0])==0: continue
                if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        print("End reading")

        # pool 
        tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], spatial_constraint_gps, duration_constraint, outputFile) for name in UserList]]

        finishit = [t.get() for t in tasks]
        '''
        for name in UserList:
            func((name, UserList[name], spatial_constraint_gps, duration_constraint, outputFile))
        '''
    pool.close()
    pool.join()

    outputFile_real = outputFile.replace('_tmp.csv','.csv')
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile,outputFile_real)
