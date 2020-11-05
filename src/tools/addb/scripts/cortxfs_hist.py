#
# Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

import argparse
import sys
from peewee import SqliteDatabase
from matplotlib import pyplot as plt
import numpy as np

DB      = SqliteDatabase(None)
BLOCK   = 16<<10
PROC_NR = 48
DBBATCH = 95
PID     = 0
rec_limit = -1
start_opid = 0
timesort = "NA"
sm_on = "NA"
filter_op_id = -1

def db_init(path):
    DB.init(path, pragmas={
        'journal_mode': 'memory',
        'cache_size': -1024*1024*256,
        'synchronous': 'off',
    })

def db_connect():
    DB.connect()

def db_close():
    DB.close()

def gen_perfc_op_hist_graph(fn_tag: str="fsal_read", op_file: str="cortxfs_perfc_graph"):
    xvals_opids=[]
    yvals_time=[]
    yvals_time_fsal=[]
    yvals_time_kvs=[]
    yvals_time_cfs=[]
    yvals_time_dstore=[]
    yvals_time_ds=[]
    yvals_time_cortx_kvs=[]
    yvals_time_m0=[]
    yvals_time_init=[]
    yvals_time_m0kvs=[]
    yvals_time_m0_key_iter=[]
    xmax = 0
    ymax = 0
    time_diff_fsal = 0
    time_diff_kvs = 0
    time_diff_cfs = 0
    time_diff_dstore = 0
    time_diff_ds = 0
    time_diff_cortx_kvs = 0
    time_diff_m0 = 0
    time_diff_init = 0
    time_diff_m0kvs = 0
    time_diff_m0_key_iter = 0
    processed_ops_clr = []
    skip_it = 0

    with DB.atomic():
        if filter_op_id != -1:
            cursor = DB.execute_sql(f"SELECT DISTINCT opid from entity_states WHERE fn_tag LIKE \"{fn_tag}\" AND opid == {filter_op_id} ORDER BY id ASC")
        elif rec_limit != -1:
            cursor = DB.execute_sql(f"SELECT DISTINCT opid from entity_states WHERE fn_tag LIKE \"{fn_tag}\" AND opid >= {start_opid} ORDER BY id ASC LIMIT {rec_limit}")
        else:
            cursor = DB.execute_sql(f"SELECT DISTINCT opid from entity_states WHERE fn_tag LIKE \"{fn_tag}\" ORDER BY id ASC")
        field_opids = list(cursor.fetchall())
    label_opids = ("o");
    opids = [dict(zip(label_opids, f)) for f in field_opids]
    # print (opids)

    if opids == []:
        print ("Could not find enough details in db for {0}", format(fn_tag))
        return

    for opid in opids:
        with DB.atomic():
            cursor = DB.execute_sql(f"SELECT * from entity_states WHERE opid = {opid['o']}")
            field_states = list(cursor.fetchall())
        label_states = ("id", "pid", "time", "tsdb_mod", "fn_tag", "sm_tag", "entity_type", "opid", "state_type");
        states = [dict(zip(label_states, f)) for f in field_states]
        # print (states)
        t0 = -1
        t1 = -1
        for state in states:
            if state['state_type'] in "finish":
                t1 = state['time']
            if state['state_type'] in "init":
                t0 = state['time']
        if t0 == -1 or t1 == -1:
            print (f"incomplete states, discarding this sample, {states}")
        else:
            time_diff = t1 - t0
            # print (f"opid {opid['o']} took {time_diff} ns to complete")
            xvals_opids.append(opid['o'])
            yvals_time.append(time_diff)
            if xmax < opid['o']:
                xmax = opid['o']
            if ymax < time_diff:
                ymax = time_diff
            if sm_on not in "NA":
                time_diff_fsal = 0
                time_diff_kvs = 0
                time_diff_cfs = 0
                time_diff_dstore = 0
                time_diff_ds = 0
                time_diff_cortx_kvs = 0
                time_diff_m0 = 0
                time_diff_init = 0
                time_diff_m0kvs = 0
                time_diff_m0_key_iter = 0
                with DB.atomic():
                    cursor = DB.execute_sql(f"SELECT DISTINCT src_opid from entity_maps where dst_opid = {opid['o']}")
                    field_sopids = list(cursor.fetchall())
                label_sopids = ("s")
                sopids = [dict(zip(label_sopids, f)) for f in field_sopids]
                for sopid in sopids:
                    with DB.atomic():
                        cursor = DB.execute_sql(f"select time,opid,state_type,sm_tag from entity_states where opid = {sopid['s']}")
                        field_sms = list(cursor.fetchall())

                        cursor = DB.execute_sql(f"select * from entity_maps where src_opid = {sopid['s']}")
                        field_sms_map = list(cursor.fetchall())

                    label_sms = ("time", "opid", "state_type", "smtag")
                    sms = [dict(zip(label_sms, f)) for f in field_sms]
                    # print (sms)
                    label_sms_map = ("id", "pid", "time", "tsdb_mod", "fn_tag", "sm_tag", "entity_type", "map_name", "src_opid", "dst_opid", "clr_opid")
                    sms_map = [dict(zip(label_sms_map, f)) for f in field_sms_map]
                    # print (sms_map[0])
                    # print (sms_map[0]['clr_opid'])
                    t0 = -1
                    t1 = -1
                    for sm in sms:
                        if sm['state_type'] in "finish":
                           t1 = sm['time']
                        if sm['state_type'] in "init":
                           t0 = sm['time']
                    if t0 == -1 or t1 == -1:
                        print (f"incomplete states, discarding this sample, {sms}")
                    else:
                        skip_it = 0
                        for item in processed_ops_clr:
                            if item['k'] == sm['smtag'] and item['v'] == sms_map[0]['clr_opid']:
                                processed_ops_clr.append(dict({'k':sm['smtag'], 'v':sm['opid']}))
                                skip_it = 1
                                break
                            if item['k'] == sm['smtag'] and item['v'] == sms_map[0]['dst_opid']:
                                processed_ops_clr.append(dict({'k':sm['smtag'], 'v':sm['opid']}))
                                skip_it = 1
                                break
                        if skip_it == 1:
                            continue
                        processed_ops_clr.append(dict({'k':sm['smtag'], 'v':sm['opid']}))
                        time_diff = t1 - t0
                        # print (f"{sm['smtag']} opid {sopid['s']} took {time_diff} ns to complete")
                        if sm['smtag'] in "PST_FSAL":
                            time_diff_fsal = time_diff_fsal + (t1 - t0)
                        elif sm['smtag'] in "CFS":
                            time_diff_kvs = time_diff_kvs + (t1 - t0)
                        elif sm['smtag'] in "FSAL_CFS":
                            time_diff_cfs = time_diff_cfs + (t1 - t0)
                        elif sm['smtag'] in "DSAL":
                            time_diff_dstore = time_diff_dstore + (t1 - t0)
                        elif sm['smtag'] in "NSAL":
                            time_diff_ds = time_diff_ds + (t1 - t0)
                        elif sm['smtag'] in "UTILS":
                            time_diff_cortx_kvs = time_diff_cortx_kvs + (t1 - t0)
                        elif sm['smtag'] in "PST_M0":
                            time_diff_m0 = time_diff_m0 + (t1 - t0)
                        elif sm['smtag'] in "PST_INIT":
                            time_diff_init = time_diff_init + (t1 - t0)
                        elif sm['smtag'] in "PST_M0KVS":
                            time_diff_m0kvs = time_diff_m0kvs + (t1 - t0)
                        elif sm['smtag'] in "PST_M0_KEY_ITER":
                            time_diff_m0_key_iter = time_diff_m0_key_iter + (t1 - t0)
                yvals_time_fsal.append(time_diff_fsal)
                yvals_time_kvs.append(time_diff_kvs)
                yvals_time_cfs.append(time_diff_cfs)
                yvals_time_dstore.append(time_diff_dstore)
                yvals_time_ds.append(time_diff_ds)
                yvals_time_cortx_kvs.append(time_diff_cortx_kvs)
                yvals_time_m0.append(time_diff_m0)
                yvals_time_init.append(time_diff_init)
                yvals_time_m0kvs.append(time_diff_m0kvs)
                yvals_time_m0_key_iter.append(time_diff_m0_key_iter)
                # print ("total_sm_time: ")
                # print (time_diff_fsal+time_diff_kvs+time_diff_cfs+time_diff_dstore+time_diff_ds+time_diff_cortx_kvs+time_diff_m0+time_diff_init+time_diff_m0kvs+time_diff_m0_key_iter)

    if timesort not in "NA":
        opid_time = dict(zip(xvals_opids, yvals_time))
        if timesort in "INCR":
            opid_time1 = dict(sorted(opid_time.items(), key=lambda x: x[1]))
        else:
            opid_time1 = dict(sorted(opid_time.items(), key=lambda x: x[1], reverse=True))
        xvals_opids = list(opid_time1.keys())
        yvals_time = list(opid_time1.values())

    x = np.arange(len(xvals_opids))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x + width/2, yvals_time, width, label='total')
    if sm_on not in "NA":
        if filter_op_id != -1:
            rects3 = ax.bar(x + width/2 + 1, yvals_time_kvs, width, label='CFS')
            rects4 = ax.bar(x + width/2 + 2, yvals_time_cfs, width, label='FSAL_CFS')
            rects5 = ax.bar(x + width/2 + 3, yvals_time_dstore, width, label='DSAL')
            rects6 = ax.bar(x + width/2 + 4, yvals_time_ds, width, label='NSAL')
            rects7 = ax.bar(x + width/2 + 5, yvals_time_cortx_kvs, width, label='UTILS')
        else:
            rects3 = ax.bar(x, yvals_time_kvs, width, label='CFS')
            rects4 = ax.bar(x, yvals_time_cfs, width, label='FSAL_CFS')
            rects5 = ax.bar(x, yvals_time_dstore, width, label='DSAL')
            rects6 = ax.bar(x, yvals_time_ds, width, label='NSAL')
            rects7 = ax.bar(x, yvals_time_cortx_kvs, width, label='UTILS')

    plt.title(f"{fn_tag} \n time")
    plt.xlabel(f"{fn_tag} opid(s)")
    plt.ylabel("time (ns)")
    ax.set_xticks(x)
    ax.set_xticklabels(xvals_opids, rotation=90, ha='center', size=3)
    ax.set_yticklabels(yvals_time, rotation=90, ha='left', size=3)
    ax.legend(loc='best', prop={'size': 6})
    plt.legend(fontsize=4)

    def autolabel(rects, yvals_time):
        """Attach a text label above each bar in *rects*, displaying its height."""
        idx = 0
        for rect in rects:
            height = rect.get_height()
            text = ax.annotate(str(yvals_time[idx]).format(height),
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, -3),  # 3 points vertical offset
            textcoords="offset points",
            ha='center', va='bottom')
            idx=idx+1
            text.set_rotation(90)
            text.set_fontsize(3)

    autolabel(rects1, yvals_time)
    if sm_on not in "NA":
        if filter_op_id != -1:
            autolabel(rects3, yvals_time_kvs)
            autolabel(rects4, yvals_time_cfs)
            autolabel(rects5, yvals_time_dstore)
            autolabel(rects6, yvals_time_ds)
            autolabel(rects7, yvals_time_cortx_kvs)
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.2)
    plt.savefig(fname=op_file, format="svg")
    print (f"Histogram graph render completed for operation {fn_tag}")

    return


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0], description="""
    cortxfs_hist.py: Display histogram graph of a provided cortxfs function.
    """)
    parser.add_argument("-d", "--db", type=str, default="cortxfs_perfc.db",
                        help="Performance database (cortxfs_perfc.db)")
    parser.add_argument("-ft", "--fn_tag", type=str,
                        help="A valid fn_tag from CORTXFS stack which is "
                        "enabled for performance profiling")
    parser.add_argument("-rl", "--rec_limit", type=str, default="10",
                        help="How many max opids to be shown in a single "
                        "graph, use 'NA' to specify no limit. Default is 10")
    parser.add_argument("-op", "--opid", type=str, default="NA",
                        help="Get histogram of a specific opid")
    parser.add_argument("-so", "--start_opid", type=str, default="0",
                        help="Along with limit, from which opid the graph needs"
                        "to be generated. Default is 0 to show from the start")
    parser.add_argument("-st", "--sort_by_time", type=str, default="NA",
                        help="Use this to time sort the returned results. "
                        "Valid i/p are: INCR, DECR, NA. Default is NA. "
                        " It can't be combined with --sub_mod_stats.")
    parser.add_argument("-sm-stats", "--sub_mod_stats", type=str, default="NA",
                        help=" 'YES' to see time stats for sub-modules."
                        "Default set to 'NA'.")
    parser.add_argument("-o", "--op_graph", type=str,
                        default="cortxfs_hist_graph.svg",
                        help="CORTXFS fn_tag histogram graph output file "
                        " (cortxfs_hist_graph.svg)")
    return parser.parse_args()

if __name__ == '__main__':
    args=parse_args()

    start_opid = int(args.start_opid)

    display_msg = f"Creating cortxfs histogram graph for fn_tag {args.fn_tag}, "

    if args.opid not in "NA":
        filter_op_id = int(args.opid)
        display_msg = display_msg + f"for opid {filter_op_id}, "
    else:
        timesort = args.sort_by_time
        display_msg = display_msg + f"timesort {timesort}, "
        if args.rec_limit not in "NA":
            rec_limit = int(args.rec_limit)
            display_msg = display_msg + f"record max limit is {rec_limit}, "

    if args.sub_mod_stats in "YES":
        sm_on = "YES"
        display_msg = display_msg + "sub module stats are enabled, "
        if timesort not in "NA":
            print ("Both --sub_mod_stats and --sort_by_time can not be used together")
            exit (1)

    print(f"{display_msg} from db file {args.db}, o/p file {args.op_graph}")

    db_init(args.db)
    db_connect()
    gen_perfc_op_hist_graph(args.fn_tag, args.op_graph)
    db_close()
