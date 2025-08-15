#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging

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
        row = [sg_id, None, event_id, event_type, state_key]
        state_table.append(row)
        lifetimes[event_id] = row

    def mark_state_as_gone(last_sg_id, event_id):
        row = lifetimes[event_id]
        row[1] = last_sg_id

    def dump_state():
        sql = """
        
        """
        c = conn.cursor()
        execute_values(
            c,
            "INSERT INTO state (start_sg_id, end_sg_id, event_id, type, state_key) VALUES %s",
            state_table,
        )
        c.execute("UPDATE state set room_id=%s", [room_id])
        c.close()

    cursor = conn.cursor()

    # grab the SG DAG into RAM for speedy access.
    logger.info("loading SG DAG")
    cursor.execute("SELECT state_group, prev_state_group FROM state_groups sg JOIN state_group_edges sge ON sg.id = sge.state_group where room_id=%s", [room_id])
    next_edges = {} # next_edges[prev_id] = [ next_ids ]
    prev_edges = {} # prev_edges[next_id] = [ prev_ids ]
    for row in cursor.fetchall():
        next_sg = next_edges.setdefault(row[1], [])
        next_sg.append(row[0])
        prev_sg = prev_edges.setdefault(row[0], [])
        prev_sg.append(row[1])

    # grab the ordered SGs and their state in one swoop, so we don't have to keep fishing out state events
    # we do the join so we can benefit from state_groups' pkey index on id when ordering
    # todo: split into batches to avoid a massive txn
    logger.info("loading SG state")
    cursor.execute("SELECT id, type, state_key, sgs.event_id FROM state_groups sg JOIN state_groups_state sgs ON sg.id=sgs.state_group WHERE sg.room_id=%s ORDER BY id", [room_id])

    # state_groups[sg_id] = { (event_type, state_key): event_id }
    state_groups = {}

    def get_state_dict(sg_id):
        if sg_id in state_groups:
            sg = state_groups[sg_id]
            if sg_id in prev_edges:
                for prev_id in prev_edges[sg_id]:
                    if prev_id in state_groups:
                        # print (f"merging: {get_state(prev_id)} with {sg}")
                        sg = get_state_dict(prev_id) | sg

                        # we can remove older SGs here if this was the only
                        # place we referred to them, effectively memoizing our
                        # results
                        if (len(next_edges[prev_id]) == 1):
                            print (f"pruning {prev_id}")
                            state_groups[sg_id] = sg
                            del state_groups[prev_id]
                            # TODO: we could also prune prev_id from prev_edges[sg_id] at this point
                            # to avoid trying to traverse into it, but it's a unlikely to help much

        else:
            sg = {}
        return sg

    state_set = set()
    last_sg_id = None
    type_dict = {}
    sg = {}
    while True:
        rows = cursor.fetchmany(10000)
        if not rows:
            break
        for (sg_id, event_type, state_key, event_id) in rows:
            print()
            print (sg_id, event_type, state_key, event_id)
            type_dict[event_id] = (event_type, state_key)

            def handle_last_sg(state_set):
                state_groups[last_sg_id] = sg
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

