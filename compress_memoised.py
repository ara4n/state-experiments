#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import tracemalloc
import gc

# tracemalloc.start()

# go through the state groups in order of SG ID
# create a new table called state:
# start_sg_id, end_sg_id, event_id, room_id, type, state_key
# calculate next state at the next sg_id (by recursing through s_g_e as in today's synapse)
# diff sets of before & after
# insert new state events which have now materialised at this point
# set end_sg_id for the current state for ones which disappear at the next row.

# Disadvantage is that we can't cache state groups, but perhaps it'll be so fast we don't need to.
# ...or we can create the state group optimisation in the cache rather than the DB on demand
# by factoring out commonality in the results coughed up by the DB.
# Other problem is that state resets which thrash the state of the room will cause the table to grow, but
# it will still be way better than the previous incarnation. Plus we may be able to detect and throw away
# thrashing as part of mitigating state spam.

DB_CONFIG = {
    # 'host': 'localhost',
    'database': 'test',
    # 'user': 'your_username',
    # 'password': 'your_password',
    # 'port': '5432'
}

# CREATE TABLE state (
#   start_sg_id bigint not null,
#   end_sg_id bigint,
#   event_id text,
#   room_id text,
#   type text,
#   state_key text
# );

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'

try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(autocommit=True)

    state_table = []
    lifetimes = {} # event_id -> ( start_sg, end_sg )

    def add_state(sg_id, event_id, event_type, state_key):
        print (f"adding {sg_id} {event_id} {event_type} {state_key}")
        row = [sg_id, None, event_id, event_type, state_key]
        state_table.append(row)
        lifetimes[event_id] = row

    def mark_state_as_gone(last_sg_id, event_id):
        print (f"marking {event_id} as gone in {last_sg_id}")
        row = lifetimes[event_id]
        row[1] = last_sg_id

    def dump_state():
        c = conn.cursor()
        execute_values(
            c,
            "INSERT INTO state (start_sg_id, end_sg_id, event_id, type, state_key) VALUES %s",
            state_table,
        )
        c.execute("UPDATE state set room_id=%s", [room_id])
        c.close()

    cursor = conn.cursor()

    # grab the SG DAG into RAM for speedy access. This is fast.
    logger.info("loading SG DAG")
    cursor.execute("SELECT state_group, prev_state_group FROM state_groups sg JOIN state_group_edges sge ON sg.id = sge.state_group where room_id=%s", [room_id])
    next_edges = {} # next_edges[prev_id] = [ next_ids ]
    prev_edges = {} # prev_edges[next_id] = [ prev_ids ]
    sg_id_set = set() # set of all state group IDs
    for row in cursor.fetchall():
        next_sg = next_edges.setdefault(row[1], [])
        next_sg.append(row[0])
        # N.B. at least for uncompressed state groups, it seems each SG only has a single prev SG.
        prev_sg = prev_edges.setdefault(row[0], [])
        prev_sg.append(row[1])
        sg_id_set.add(row[0])
        sg_id_set.add(row[1])

    # grab the ordered SGs and their state in one swoop, so we don't have to keep fishing out state events.
    # Problem: selects from sg or sgs table ordered by SG ID is slow as there's no index on both room_id and SG ID.
    #
    # so instead, let's load in batches of 5000 (which is good anyway), explicitly querying the rows from the
    # ordered state_group_edges table.
    logger.info("loading SG state")

    # state_groups[sg_id] = { (event_type, state_key): event_id }
    state_groups = {}

    state_set = set()
    last_sg_id = None
    type_dict = {}
    sg = {}

    def get_state_dict(sg_id):
        if sg_id in state_groups:
            sg = state_groups[sg_id]
            if sg_id in prev_edges:
                prevs = prev_edges[sg_id]
                for prev_id in prevs:
                    if prev_id in state_groups:
                        # print (f"merging: {get_state(prev_id)} with {sg}")
                        sg = get_state_dict(prev_id) | sg

                        # we can remove older SGs here if this was the only
                        # place we referred to them, effectively memoizing our
                        # results.
                        # on #nvi, this increases our speed by 2x and reduces our peak RAM by 2.5x
                        print (f"merging sg {prev_id} into sg {sg_id}")
                        state_groups[sg_id] = sg

                        # if (len(next_edges[prev_id]) == 1):
                        #     print (f"purging fully merged sg {prev_id}")
                        #     del state_groups[prev_id]
                        #     # as an optimisation, we deliberately skip clearing up next_edges and prev_edges as
                        #     # we know we won't refer to this SG again.
                        #     # In practice, this seems to buy us very little (but costs a bunch of RAM)
                        # else:
                        #     next_edges[prev_id] = [ id for id in next_edges[prev_id] if id != sg_id ]
                        #     prev_edges[sg_id] = [ id for id in prev_edges[sg_id] if id != prev_id ]

                        next_edges[prev_id] = [ id for id in next_edges[prev_id] if id != sg_id ]
                        prev_edges[sg_id] = [ id for id in prev_edges[sg_id] if id != prev_id ]
                        if (len(next_edges[prev_id]) == 0):
                            print (f"purging fully merged sg {prev_id}")
                            del state_groups[prev_id]

            if sg_id not in next_edges:
                print (f"purging dead-end sg {sg_id}")
                del state_groups[sg_id]
        else:
            sg = {}
        return sg

    sg_id_list = sorted(sg_id_set)
    del sg_id_set
    batch_size = 5000
    for i in range(0, len(sg_id_list), batch_size):
        slice = sg_id_list[i:i+batch_size]
        #print (slice)

        cursor.execute("""
            SELECT state_group, type, state_key, event_id
            FROM state_groups_state 
            WHERE state_group = ANY(%s)
            ORDER BY state_group
        """, [slice])

        for (sg_id, event_type, state_key, event_id) in cursor.fetchall():
            print()
            print("Checking", sg_id, event_type, state_key, event_id)
            print()
            type_dict[event_id] = (event_type, state_key)

            def handle_last_sg(state_set):
                state_groups[last_sg_id] = sg
                print(f"Handling sg {last_sg_id}")
                print(f"prev_edges[{last_sg_id}] = { prev_edges.get(last_sg_id, None) }")
                for prev in prev_edges.get(last_sg_id, []):
                    print(f"next_edges[{prev}] = { next_edges.get(prev, None) }")
                #print ("sg: ", sg)
                #print ("state: ", get_state(last_sg_id))

                new_state_set = set(get_state_dict(last_sg_id).values())
                #print(f"last_sg_id: {last_sg_id}, state_groups[] = {sg}, new_state_set: {new_state_set}")
                #print ("state_set: ", state_set)
                #print ("new_state_set: ", new_state_set)

                new_ids = new_state_set - state_set
                gone_ids = state_set - new_state_set
                print("new_ids", new_ids)
                print("gone_ids", gone_ids)
                for id in new_ids:
                    (et, esk) = type_dict[id]
                    add_state(last_sg_id, id, et, esk)
                for id in gone_ids:
                    mark_state_as_gone(last_sg_id, id)
                return new_state_set

            # build up the event IDs in this state group
            if sg_id == last_sg_id:
                sg[(event_type, state_key)] = event_id
                continue
            else:
                if last_sg_id is not None:
                    state_set = handle_last_sg(state_set)

                # get going on the new sg
                last_sg_id = sg_id
                sg = { (event_type, state_key): event_id }

    # flush the last sg
    handle_last_sg(state_set)

    # import pprint
    # print("state_groups")
    # pprint.pp(state_groups)
    # print("prev_edges")
    # pprint.pp(prev_edges)
    # print("next_edges")
    # pprint.pp(next_edges)

    # finally, dump the state table to the DB.
    dump_state()

    # then, we can query current state with:
    # select * from state where end_sg_id is null;
    #
    # or for historic state, with:
    # select * from state where start_sg_id <= 747138778 and (end_sg_id is null or end_sg_id > 747138778);

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# print("gc memory", gc.collect())

# current, peak = tracemalloc.get_traced_memory()
# print(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
# print(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")
# tracemalloc.stop()

# Our existing synapse schema, for reference

# CREATE TABLE state_groups_state (
#     state_group bigint,
#     room_id text,
#     type text,
#     state_key text,
#     event_id text
# );
# CREATE INDEX state_groups_state_room_id_idx ON state_groups_state USING brin (room_id) WITH (pages_per_range='1');
# CREATE INDEX state_groups_state_type_idx_new ON state_groups_state USING btree (state_group, type, state_key);

# CREATE TABLE state_groups (
#     id bigint NOT NULL,
#     room_id text NOT NULL,
#     event_id text NOT NULL
# );
# ALTER TABLE ONLY state_groups
#     ADD CONSTRAINT state_groups_pkey PRIMARY KEY (id);
# CREATE INDEX state_groups_room_id_idx ON state_groups USING btree (room_id);

# CREATE TABLE state_group_edges (
#     state_group bigint NOT NULL,
#     prev_state_group bigint NOT NULL
# );
# CREATE INDEX state_group_edges_prev_idx ON state_group_edges USING btree (prev_state_group);
# CREATE UNIQUE INDEX state_group_edges_unique_idx ON state_group_edges USING btree (state_group, prev_state_group);

