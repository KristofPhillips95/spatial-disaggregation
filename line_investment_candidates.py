from nordic490 import N490
import sys
import time
import pandas as pd
import datetime as dt
# m = N490(year=2018)
# m.branch_params() # simple assumptions on R, X and B
#
# t = time.time()
# load, gen, link = m.get_measurements('20180120:18','20180120:20' ) # download data for a certain period
# print(time.time()-t)
#
#
# m.distribute_power(load, gen, link,time=0)  # distribute on buses and gens (simple method)
# m.dcpf() # solve DC power flow
#
#
# m.distribute_power(load, gen, link,time=1)  # distribute on buses and gens (simple method)
# m.dcpf()
#
#

# nb_timesteps = load.shape[0]
start = dt.datetime(year=2018,month=1,day=1,hour=1)
end = dt.datetime(year=2018,month=12,day=31,hour=23)


start_str = start.strftime('%Y%m%d') + ':' +start.strftime('%H')
time_range = {}
date_time = start
while date_time <= end:
    date_time_str = date_time.strftime('%Y%m%d') + ':' +date_time.strftime('%H')
    time_range[date_time] = date_time_str
    date_time = date_time.__add__(dt.timedelta(hours=1))


for time in time_range:
    m = N490(year=2018)
    m.branch_params()
    try:
        load, gen, link = m.get_measurements(time_range[time])
        m.distribute_power(load,gen,link)
        m.dcpf()
        if time == start:
            flow_frame = pd.DataFrame(data=m.line.Cap)
        mapper = {"flow": time}
        flow_frame_this_timestep = pd.DataFrame(data=m.line.flow).rename(columns=mapper)
        flow_frame = flow_frame.merge(flow_frame_this_timestep, right_index=True, left_index=True)
    except:
        print(time_range[time]  + "failed")
    print(time)


usage_rate_frame = flow_frame.iloc[:,1:].div(flow_frame.Cap, axis=0)
acummulated_usage =  usage_rate_frame.abs().sum(axis=1)
nb_times_congested = usage_rate_frame[usage_rate_frame.abs()>1].count(axis=1)

rank_usage = acummulated_usage.rank(method = 'average', ascending = False)
rank_congested = nb_times_congested.rank(method = 'average',ascending = False)

flow_frame.to_excel('flow_frame_2018.xlsx')
rank = rank_usage + rank_congested
rank = rank.sort_values()
rank_df = pd.DataFrame(data = rank)
rank.to_excel('Investment_candidates.xlsx')