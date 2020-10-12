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

DB      = SqliteDatabase(None)
BLOCK   = 16<<10
PROC_NR = 48
DBBATCH = 95
PID     = 0

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
    xmax = 0
    ymax = 0

    with DB.atomic():
        cursor = DB.execute_sql(f"SELECT DISTINCT opid from entity_states WHERE fn_tag LIKE \"{fn_tag}\" ORDER BY id ASC")
        field_opids = list(cursor.fetchall())
    label_opids = ("o");
    opids = [dict(zip(label_opids, f)) for f in field_opids]

    if opids == []:
        print ("Could not find enough details in db for {0}", format(fn_tag))
        return

    for opid in opids:
        with DB.atomic():
            cursor = DB.execute_sql(f"SELECT * from entity_states WHERE opid = {opid['o']}")
            field_states = list(cursor.fetchall())
        label_states = ("id", "pid", "time", "tsdb_mod", "fn_tag", "entity_type", "opid", "state_type");
        states = [dict(zip(label_states, f)) for f in field_states]
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
            # print (f"opid {opid['o']} took {time_diff} us to complete")
            xvals_opids.append(opid['o'])
            yvals_time.append(time_diff)
            if xmax < opid['o']:
                xmax = opid['o']
            if ymax < time_diff:
                ymax = time_diff

    fig, ax = plt.subplots()
    ax.bar([idx for idx in range(len(xvals_opids))], yvals_time, align='edge', color='Red', width=0.3)
    ax.set_xticks([idx for idx in range(len(xvals_opids))])
    ax.set_xticklabels(xvals_opids, rotation=90, ha='center', size=3)
    ax.set_yticklabels(yvals_time, rotation=90, ha='left', size=3)
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.2)
    plt.title(f"{fn_tag} \n time")
    plt.xlabel(f"{fn_tag} opid(s)")
    plt.ylabel("time (us)")
    plt.tight_layout()
    plt.savefig(fname=op_file, format="svg")
    print (f"Histogram graph render completed for operation {fn_tag}")

    return


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0], description="""
    cortxfs_hist.py: Display histogram graph of a provided cortxfs function.
    """)
    parser.add_argument("-d", "--db", type=str, default="cortxfs_perfc.db",
                        help="Performance database (cortxfs_perfc.db)")
    parser.add_argument("fn_tag", type=str, help="A valid fn_tag from CORTXFS stack which is enabled for performance profiling")
    parser.add_argument("-o", "--op_graph", type=str, default="cortxfs_hist_graph.png",
                        help="CORTXFS fn_tag histogram graph output file (cortxfs_hist_graph.png)")
    return parser.parse_args()

if __name__ == '__main__':
    args=parse_args()
    db_init(args.db)
    db_connect()
    print('Creating cortxfs histogram graph for fn_tag {0}, from db file {1}, o/p graph file {2}'. format(args.fn_tag, args.db, args.op_graph))
    gen_perfc_op_hist_graph(args.fn_tag, args.op_graph)
    db_close()
