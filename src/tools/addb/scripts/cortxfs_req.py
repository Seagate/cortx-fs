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

import sys
from typing import Dict
from graphviz import Digraph
import argparse
from peewee import SqliteDatabase

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

def graph_node_add(g: Digraph, name: str, header: str, attrs: Dict):
    node_template="""<
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
  <TR>
    <TD>{}</TD>
  </TR>
  <TR>
    <TD>{}</TD>
  </TR>
</TABLE>>
"""
    label = "<BR/>".join([f"{k}={v}" for (k,v) in attrs.items()])
    g.node(name, node_template.format(header, label))

def gen_perfc_op_call_graph(fsal_op_id: int=None, op_file: str="cortxfs_perfc_graph"):
    ext_graph=None
    label_maps = ("id", "pid", "time", "tsdb_mod", "fn_tag", "sm_tag", "entity_type", "map_name", "src_opid", "dst_opid", "clr_opid");
    label_attrs = ("id", "pid", "time", "tsdb_mod", "fn_tag", "sm_tag", "entity_type", "opid", "attr_name", "attr_val");
    label_states = ("id", "pid", "time", "tsdb_mod", "fn_tag", "sm_tag", "entity_type", "opid", "state_type");

    with DB.atomic():
        cursor = DB.execute_sql(f"SELECT * from entity_states WHERE opid IN(SELECT ES.opid FROM entity_states ES JOIN entity_maps EM ON EM.src_opid = ES.opid AND EM.dst_opid = {fsal_op_id} OR ES.opid = {fsal_op_id} GROUP BY ES.opid) ORDER BY id ASC")
        field_states = list(cursor.fetchall())

        cursor = DB.execute_sql(f"SELECT * from entity_attributes WHERE opid IN(SELECT EA.opid FROM entity_attributes EA JOIN entity_maps EM ON EM.src_opid = EA.opid AND EM.dst_opid = {fsal_op_id} OR EA.opid = {fsal_op_id} GROUP BY EA.opid) ORDER BY id ASC")
        field_attrs = list(cursor.fetchall())

        cursor = DB.execute_sql(f"SELECT * from entity_maps where dst_opid = {fsal_op_id} ORDER BY id ASC")
        field_maps = list(cursor.fetchall())

    states = [dict(zip(label_states, f)) for f in field_states]
    attrs = [dict(zip(label_attrs, f)) for f in field_attrs]
    maps = [dict(zip(label_maps, f)) for f in field_maps]

    if states == [] or attrs == [] or maps == []:
        print ("Could not find enough details in db for OP ID", fsal_op_id)
        return

    graph = ext_graph if ext_graph is not None else Digraph(
        strict=True, format='png', node_attr = {'shape': 'plaintext'})

    attr_time_end_id = 0
    for state in states:
        if state['opid'] == fsal_op_id:
            if state['state_type'] in "init":
                # print (state['time'], state['fn_tag'], state['state_type'])
                node_content = {'op start time': state['time']}
                for attr in attrs:
                    if attr['opid'] == state['opid']:
                        # print (attr['attr_name'], attr['attr_val'])
                        node_content.update({'opid' : attr['opid'], 'parent opid' : 0, attr['attr_name']:attr['attr_val']})
                # print (node_content)
                t0 = state['time']
                graph_node_add(graph, f"init{state['opid']}", "{} {}".format(state['fn_tag'], state['state_type']), node_content)
                graph, attr_time_end_id = m0callgraph(graph, state, attrs)

            if state['state_type'] in "finish":
                # print (state['time'], state['fn_tag'], state['state_type'])
                node_content = {'opid' : attr['opid'], 'parent opid' : 0, 'op end time': state['time']}
                # print (node_content)
                t1 = state['time']
                graph_node_add(graph, f"finish{state['opid']}", "{} {}".format(state['fn_tag'], state['state_type']), node_content)
                graph.edge(f"init{state['opid']}", f"finish{state['opid']}", label="{} ns".format(str(t1 - t0)))
                if attr_time_end_id != 0:
                    graph.edge( f"m0childfinish{attr_time_end_id}", f"finish{state['opid']}")

    for mp in maps:
        attr_time_end_id = 0
        for state in states:
            if state['opid'] == mp['src_opid']:
                if state['state_type'] in "init":
                    # print (state['time'], state['fn_tag'], state['state_type'])
                    node_content = {'op start time': state['time']}
                    for attr in attrs:
                        if attr['opid'] == state['opid']:
                            # print (attr['attr_name'], attr['attr_val'])
                            node_content.update({'opid' : mp['src_opid'], 'parent opid' : mp['clr_opid'], attr['attr_name']:attr['attr_val']})
                    # print (node_content)
                    t0 = state['time']
                    graph_node_add(graph, f"childinit{state['opid']}", "{} {}".format(state['fn_tag'], state['state_type']), node_content)
                    graph, attr_time_end_id = m0callgraph(graph, state, attrs)

                if state['state_type'] in "finish":
                    # print (state['time'], state['fn_tag'], state['state_type'])
                    node_content = {'opid' : mp['src_opid'], 'parent opid' : mp['clr_opid'], 'op end time': state['time']}
                    # print (node_content)
                    t1 = state['time']
                    graph_node_add(graph, f"childfinish{state['opid']}", "{} {}".format(state['fn_tag'], state['state_type']), node_content)
                    graph.edge(f"childinit{state['opid']}", f"childfinish{state['opid']}", label="{} ns".format(str(t1 - t0)))
                    if attr_time_end_id != 0:
                        graph.edge( f"m0childfinish{attr_time_end_id}", f"childfinish{state['opid']}")

    for mp in maps:
        if mp['clr_opid'] == fsal_op_id:
            graph.edge(f"init{mp['clr_opid']}", f"childinit{mp['src_opid']}")
            graph.edge(f"childfinish{mp['src_opid']}", f"finish{mp['clr_opid']}")
        else:
            graph.edge(f"childinit{mp['clr_opid']}", f"childinit{mp['src_opid']}")
            graph.edge(f"childfinish{mp['src_opid']}", f"childfinish{mp['clr_opid']}")

    if ext_graph is None:
        graph.render(op_file)

    print ("Graph render completed")

    return

def m0callgraph(graph, state, attrs):
    attr_time_start_id = 0
    attr_time_end_id = 0
    for attr in attrs:
        if attr['opid'] == state['opid']:
            if attr['attr_name'].find("attr_time_start") != -1:
                m0_node_content = {'op start time': attr['time']}
                m0_t0 = attr['time']
                graph_node_add(graph, f"m0childinit{attr['id']}", attr['attr_name'], m0_node_content)
                attr_time_start_id = attr['id']
                if attr_time_end_id == 0:
                    graph.edge(f"childinit{state['opid']}", f"m0childinit{attr_time_start_id}")
                else:
                    graph.edge(f"m0childfinish{attr_time_end_id}", f"m0childinit{attr_time_start_id}")
            if attr['attr_name'].find("attr_time_end") != -1:
                m0_node_content = {'op end time': attr['time']}
                m0_t1 = attr['time']
                graph_node_add(graph, f"m0childfinish{attr['id']}", attr['attr_name'], m0_node_content)
                attr_time_end_id = attr['id']
                graph.edge(f"m0childinit{attr_time_start_id}", f"m0childfinish{attr_time_end_id}", label="{} ns".format(str(m0_t1 - m0_t0)))
    return graph, attr_time_end_id

def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0], description="""
    cortxfs_req.py: Display significant performance counters for cortxfs stack.
    """)
    parser.add_argument("-d", "--db", type=str, default="cortxfs_perfc.db",
                        help="Performance database (cortxfs_perfc.db)")
    parser.add_argument("fsal_op_id", type=str, help="Operation id whose perf counters needs to be checked")
    parser.add_argument("-o", "--op_graph", type=str, default="cortxfs_perfc_graph.png",
                        help="CORTXFS Perfc operation callgraph output file (cortxfs_perfc_graph.png)")
    return parser.parse_args()

if __name__ == '__main__':
    args=parse_args()
    fsal_op_id = int(args.fsal_op_id)
    db_init(args.db)
    db_connect()
    print('Creating cortxfs performance call graphs for fsal_op_id {0}, from db file {1}, o/p graph png file {2}'. format(fsal_op_id, args.db, args.op_graph))
    gen_perfc_op_call_graph(fsal_op_id, args.op_graph)
    db_close()
