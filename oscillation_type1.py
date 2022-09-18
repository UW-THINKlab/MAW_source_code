##9/11/2021 diagnosis
#1. If a pair of traces appears multiple times in a trajectory, and one of them is identified as oscillation, then all occurances of this pair will be "corrected" (even if other occurances are not osscilation).
#2. Only stay locations are corercted. Traces with oscillations are not dealt with.
#3. Oscilation is required to bound back to the exact same location.
#4. Line 115 should also consider trace locations (i.e. adding a trace to a stay)

"""
remove oscillation traces
:param user:
:param dur_constr:
:return: oscill gps pair list
"""
import sys, func_timeout
#import geopy.distance
from distance import distance

def oscillation_h1_oscill(user, dur_constr):
    # user = user#arg[0]
    TimeWindow = dur_constr#arg[1]#5 * 60

    tracelist = [] # we will be working on tracelist not user
    for d in sorted(user.keys()):
        user[d].sort(key=lambda x: int(x[0]))
        for trace in user[d]:
            dur_i = 1 if int(trace[9]) == -1 else int(trace[9]) # duration is 1 for passing-by records
            lat = trace[3] if float(trace[6]) == -1 else trace[6]
            long = trace[4] if float(trace[7]) == -1 else trace[7]
            rad = trace[5] if int(trace[8]) == -1 else trace[8]
            stay_not = 0 if float(trace[6]) == -1 else 1
            tracelist.append([trace[1], trace[0], dur_i, lat, long, rad, stay_not])
            # format of each record: [ID, time, duration, lat, long, uncertainty_radius]

    # integrate: only one record representing one stay (i-i records)
    orig_index = []
    i = 0
    while i < len(tracelist) - 1:
        orig_index.append(i)
        if tracelist[i + 1][2:5] == tracelist[i][2:5]:
            del tracelist[i + 1]
        else:
            i += 1
    orig_index.append(i)

    # get gps list from tracelist
    gpslist = [(trace[3], trace[4]) for trace in tracelist]
    # unique gps list
    uniqList = list(set(gpslist))
    gps_dur_count = {item: 0 for item in uniqList}
    for tr in tracelist:
        if int(tr[2]) == 0:
            gps_dur_count[(tr[3],tr[4])] += 1
        else:
            gps_dur_count[(tr[3],tr[4])] += int(tr[2])

    # All prepared
    oscillation_pairs = []
    t_start = 0

    # replace pong by ping; be aware that "tracelistno_original==tracelist"
    flag_find_circle = False
    while t_start < len(tracelist):
        flag_find_circle = False
        suspSequence = []
        suspSequence.append(t_start)
        for t in range(t_start + 1, len(tracelist)):  # get the suspicious sequence
            if int(tracelist[t][1]) <= int(tracelist[t_start][1]) + int(tracelist[t_start][2]) + TimeWindow:
                suspSequence.append(t)
                # loc1 = (float(tracelist[t][3]),float(tracelist[t][4]))
                # loc2 = (float(tracelist[t_start][3]),float(tracelist[t_start][4]))
                if tracelist[t][3:5] == tracelist[t_start][3:5]: #geopy.distance.geodesic(loc1, loc2).meters < 300:    ####################
                    flag_find_circle = True
                    break
            else:
                break

        # check circles
        if flag_find_circle == True and len(suspSequence) > 2:  # not itself ########################
            oscillation_pairs.append(suspSequence)
            t_start = suspSequence[-1]  # + 1
        else:
            t_start += 1

    replacement = {}
    for pair in oscillation_pairs:
        stay_indicators = [tracelist[x][-1] for x in pair]
        if 1 in stay_indicators:
            replacing = sorted(pair, key=lambda x: gps_dur_count[(tracelist[x][3],tracelist[x][4])])[-1]
        else:
            replacing = pair[0]
        for to_be_replaced in pair:
            replacement[to_be_replaced] = replacing

    # find pong in trajactory, and replace it with ping
    # this part is original outside this function
    # OscillationPairList is in format: {, (ping[0], ping[1]): (pong[0], pong[1])}
    ind = 0
    for d in sorted(user.keys()):
        for trace in user[d]:
            if orig_index[ind] in replacement:
                candidate_trace = tracelist[replacement[orig_index[ind]]]
                if candidate_trace[-1] == 1:
                    trace[6], trace[7] = candidate_trace[3], candidate_trace[4]
                    trace[3], trace[4] = candidate_trace[3], candidate_trace[4]
                else:
                    trace[3], trace[4] = candidate_trace[3], candidate_trace[4]
            ind += 1

    return user #oscillgpspairlist
