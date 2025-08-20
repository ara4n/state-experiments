#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
from collections import defaultdict, deque

# CREATE TABLE state (
#   start_index bigint not null,
#   end_index bigint,
#   start_sg_id bigint not null,
#   end_sg_id bigint,
#   event_id text,
#   room_id text,
#   type text,
#   state_key text
# );

# CREATE INDEX matthew_state_end_sg_id_start_sg_id_room_id_idx ON matthew_state (end_sg_id, start_sg_id, room_id);
# took 50s on a 13GB table on matrix.org to create a ~1GB index
#
# partial index specifically on NULLs
# CREATE INDEX matthew_state_room_id_start_sg_id_null_end_sg_id_idx ON matthew_state (room_id, start_sg_id) WHERE end_sg_id IS NULL;
#
# CREATE INDEX matthew_state_event_id_idx ON matthew_state (event_id);
# just for rapid searching on events; this is big though (~1G)
#
# CREATE INDEX matthew_state_room_id_idx ON matthew_state (room_id);
# just for rapid searching on rooms

logger = logging.getLogger()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'

conn = psycopg2.connect("dbname=test")
conn.set_session(autocommit=True)

state_table = []
lifetimes = {} # event_id -> ( start_sg, end_sg )

def add_state(index, sg_id, event_id):
    logger.debug(f"adding {index} {sg_id} {event_id}")
    row = [index, None, sg_id, None, event_id, room_id]
    state_table.append(row)
    lifetimes[event_id] = row

def mark_state_as_gone(last_index, last_sg_id, event_id):
    logger.debug(f"marking {event_id} as gone at index {last_index} sg_id {last_sg_id}")
    row = lifetimes[event_id]
    row[1] = last_index
    row[3] = last_sg_id

def dump_state():
    c = conn.cursor()
    execute_values(
        c,
        "INSERT INTO state (start_index, end_index, start_sg_id, end_sg_id, event_id, room_id) VALUES %s",
        state_table,
        page_size=1000,
    )
    c.execute("""
        UPDATE state SET type=sq.type, state_key=sq.state_key
        FROM (SELECT event_id, type, state_key FROM state_groups_state) AS sq
        WHERE state.event_id = sq.event_id
    """)
    c.close()

cursor = conn.cursor()

# grab the SG DAG into RAM for speedy access. This is fast.
logger.info("loading SG DAG")
cursor.execute("SELECT state_group, prev_state_group FROM state_groups sg JOIN state_group_edges sge ON sg.id = sge.state_group where room_id=%s", [room_id])
next_edges = {} # next_edges[prev_id] = [ next_ids ]
prev_edges = {} # prev_edges[next_id] = [ prev_ids ]
for row in cursor.fetchall():
    next_sg = next_edges.setdefault(row[1], [])
    next_sg.append(row[0])
    # N.B. at least for uncompressed state groups, it seems each SG only has a single prev SG.
    prev_sg = prev_edges.setdefault(row[0], [])
    prev_sg.append(row[1])

def get_state_dict(sg_id):
    if sg_id in state_groups:
        sg = state_groups[sg_id]
        if sg_id in prev_edges:
            prevs = prev_edges[sg_id]
            for prev_id in prevs:
                # logger.debug(f"merging: {get_state(prev_id)} with {sg}")
                sg = get_state_dict(prev_id) | sg

                # we can't memoize because that relies on SGs being deleted after we're done with them,
                # whereas here we process SGs out of order.
        return sg
    else:
        logger.info(f"failed to find sg {sg_id}; must be misordered, fetching from DB")
        cursor.execute("""
            SELECT type, state_key, event_id
            FROM state_groups_state 
            WHERE state_group = %s
            ORDER BY state_group
        """, [sg_id])
        sg = {}
        for (event_type, state_key, event_id) in cursor.fetchall():
            sg[(event_type, state_key)] = event_id
        state_groups[sg_id] = sg
        return get_state_dict(sg_id)

logger.info("loading SG state")

# state_groups[sg_id] = { (event_type, state_key): event_id }
state_groups = {}

state_set = set() # the set of event_ids in current state as of last_sg_id
last_sg_id = None # the SG id being accumulated
sg = {} # sg[(type,key)] = event_id. the current stategroup being accumulated (with id last_sg_id)
type_dict = {} # type_dict[evemt_id] = (type,key) for remembering the type of a given event id

cursor.execute("select sg_id from minhashes order by ordering")
sg_id_list = cursor.fetchall()

# to visualise the resulting reordering:
# select * from (select branch, sg_id, sg_id-lag(sg_id) over (order by branch, sg_id) as l from minhashes order by branch, sg_id) l where l.l<0;
#
# XXX: THIS IS WRONG, as we haven't linearised the branches right
# In practice, this now compresses down to 8281 rows (compared to 8214 rows for Kahn ordering, somehow)
# relative to 8436 without the reordering.  The flipflopping now looks like:
#
# select start_index, start_sg_id, count(*) from state group by start_index, start_sg_id having count(*)>10 order by start_index;

batch_size = 100
index = 0
for i in range(0, len(sg_id_list), batch_size):
    slice = sg_id_list[i:i+batch_size]
    #logger.debug(slice)

    logger.info(f"i={i}, (sg {sg_id_list[i]})")

    cursor.execute("""
        SELECT state_group, type, state_key, event_id
        FROM state_groups_state 
        WHERE state_group = ANY(%s)
        ORDER BY state_group
    """, [slice])

    for (sg_id, event_type, state_key, event_id) in cursor.fetchall():
        logger.debug('')
        logger.debug(f"Checking {sg_id} {event_type} {state_key} {event_id}")
        logger.debug('')
        type_dict[event_id] = (event_type, state_key)

        def handle_last_sg(state_set, index):
            state_groups[last_sg_id] = sg
            logger.debug(f"Handling sg {last_sg_id}")
            logger.debug(f"prev_edges[{last_sg_id}] = { prev_edges.get(last_sg_id, None) }")
            for prev in prev_edges.get(last_sg_id, []):
                logger.debug(f"next_edges[{prev}] = { next_edges.get(prev, None) }")

            new_state_set = set(get_state_dict(last_sg_id).values())

            new_ids = new_state_set - state_set
            gone_ids = state_set - new_state_set
            logger.debug(f"new_ids {new_ids}")
            logger.debug(f"gone_ids {gone_ids}")
            for id in new_ids:
                add_state(index, last_sg_id, id)
            for id in gone_ids:
                mark_state_as_gone(index, last_sg_id, id)
            return new_state_set

        # build up the event IDs in this state group
        if sg_id == last_sg_id:
            sg[(event_type, state_key)] = event_id
            continue
        else:
            if last_sg_id is not None:
                state_set = handle_last_sg(state_set, index)
                index = index + 1

            # get going on the new sg
            last_sg_id = sg_id
            sg = { (event_type, state_key): event_id }

# flush the last sg
handle_last_sg(state_set, index)

# finally, dump the state table to the DB.
dump_state()
