'''
Update the duration of each stay information of one user
if the duration is smaller than duration constraint threshold, remove the point from the cluster center 

input:
    user stay information
    duration constraint threshold 
outout:
    updated user stay information
'''

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

def update_duration(user, dur_constr):
    """
        :param user:
        :return:
    """
    for d in user.keys():
        for trace in user[d]: trace[9] = -1  # clear needed! #modify grid
        i = 0
        j = i
        while i < len(user[d]):
            if j >= len(user[d]):  # a day ending with a stay, j goes beyond the last observation
                dur = str(int(user[d][j - 1][0]) + max(0, int(user[d][j - 1][9])) - int(user[d][i][0]))
                for k in range(i, j, 1):
                    user[d][k][9] = dur
                break
            if user[d][j][6] == user[d][i][6] and user[d][j][7] == user[d][i][7] and j < len(user[d]):
                j += 1
            else:
                dur = str(int(user[d][j - 1][0]) + max(0, int(user[d][j - 1][9])) - int(user[d][i][0]))
                for k in range(i, j, 1):
                    user[d][k][9] = dur
                i = j

    for d in user.keys():
        for trace in user[d]:
            # those trace with gps as -1,-1 (not clustered) should not assign a duration
            # print(trace)
            # if(str(trace[6])[-2]=='.'): continue
            if float(trace[6]) == -1: trace[9] = -1
            ## our default output format: give -1 to non-stay records
            if float(trace[9]) < dur_constr: # change back keep full trajectory: do not use center for those are not stays
                trace[6], trace[7], trace[8], trace[9] = -1, -1, -1, -1  # for no stay, do not give center

    return user


def func(args):
    name, user, dur_constraint, outputFile = args
    try:
        user = update_duration(user,dur_constraint)
        
        with lock:
            f = open(outputFile, 'a')
            writeCSV = csv.writer(f, delimiter=',')

            for day in sorted(user.keys()):
                for trace in user[day]:
                    trace[1] = name
                    writeCSV.writerow(trace)
            f.close()
    except:
        return

if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    duration_constraint = int(sys.argv[3])


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
                #if not len(row) ==12 or len(row[0])==0: continue
                #if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                #if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2): continue
                #if (('-' in row[6]) and (not row[6][0]=='-')) or (('-' in row[7]) and (not row[7][0]=='-')): continue
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        print("End reading")

        # pool 
        tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], duration_constraint, outputFile) for name in UserList]]

        finishit = [t.get() for t in tasks]

        '''
        for name in UserList:
            func((name, UserList[name], duration_constraint, outputFile))
        '''

    pool.close()
    pool.join()

    outputFile_real = outputFile.replace('_tmp.csv','.csv')
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile,outputFile_real)

