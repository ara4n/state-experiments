#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
import numpy as np
from datasketch import MinHashLSH, MinHash

# Go through each SG chronologically, calculating:
#  * current state set as of that SG
#  * its minhash
#  * its LSH bands
#  * how many events got added and removed as of that SG
# and store it in a minhashes table so we can then use it to reorder the SG based on similarity

DB_CONFIG = {
    'database': 'test',
}

# CREATE TABLE minhashes (
#   sg_id bigint,
#   room_id text,
#   minhash integer[], -- 128 minhash values
#   lsh_bands integer[], -- 16 LSH bands (hashed from the above), so 16 hashes of 8 minhash values
#   add_count int,
#   gone_count int
# );
#
# CREATE INDEX idx_lsh_bands ON minhashes USING GIN (lsh_bands);

logger = logging.getLogger()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

#room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'
room_id = '!OGEhHVWSdvArJzumhm:matrix.org'

conn = psycopg2.connect(**DB_CONFIG)
conn.set_session(autocommit=True)

table = []

def add_row(sg_id, minhash, add_count, gone_count):
    logger.debug(f"adding {sg_id} {add_count} {gone_count}")
    row = [sg_id, room_id, minhash, add_count, gone_count]
    table.append(row)

def dump_state():
    c = conn.cursor()
    execute_values(
        c,
        "INSERT INTO minhashes (sg_id, room_id, minhash, add_count, gone_count) VALUES %s",
        table,
        page_size=1000,
    )

    # FIXME: lots of scope for memoizing obviously
    c.execute("""
    UPDATE minhashes
    SET lsh_bands = ARRAY[
        hash_array(minhash[1:8]),
        hash_array(minhash[9:16]),
        hash_array(minhash[17:24]),
        hash_array(minhash[25:32]),
        hash_array(minhash[33:40]),
        hash_array(minhash[41:48]),
        hash_array(minhash[49:56]),
        hash_array(minhash[57:64]),
        hash_array(minhash[65:72]),
        hash_array(minhash[73:80]),
        hash_array(minhash[81:88]),
        hash_array(minhash[89:96]),
        hash_array(minhash[97:104]),
        hash_array(minhash[105:112]),
        hash_array(minhash[113:120]),
        hash_array(minhash[121:128])
    ];
    """)
    c.close()

    # query as:
    # select sg_id,add_count,gone_count,ARRAY(SELECT LPAD(TO_HEX(x), 8, '0') FROM UNNEST(lsh_bands) AS x) from minhashes order by sg_id;


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

def get_state_dict(sg_id):
    if sg_id in state_groups:
        sg = state_groups[sg_id]
        if sg_id in prev_edges:
            prevs = prev_edges[sg_id]
            for prev_id in prevs:
                if prev_id in state_groups:
                    # logger.debug(f"merging: {get_state(prev_id)} with {sg}")
                    sg = get_state_dict(prev_id) | sg

                    # we can remove older SGs here if this was the only
                    # place we referred to them, effectively memoizing our
                    # results.
                    # on #nvi, this increases our speed by 2x and reduces our peak RAM by 2.5x
                    logger.debug(f"merging sg {prev_id} into sg {sg_id}")
                    state_groups[sg_id] = sg

                    # if (len(next_edges[prev_id]) == 1):
                    #     logger.debug(f"purging fully merged sg {prev_id}")
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
                        logger.debug(f"purging fully merged sg {prev_id}")
                        del state_groups[prev_id]

        if sg_id not in next_edges:
            logger.debug(f"purging dead-end sg {sg_id}")
            del state_groups[sg_id]
    else:
        sg = {}
    return sg

# grab the ordered SGs and their state in one swoop, so we don't have to keep fishing out state events.
# Problem: selects from sg or sgs table ordered by SG ID is slow as there's no index on both room_id and SG ID.
#
# so instead, let's load in batches of 5000 (which is good anyway), explicitly querying the rows from the
# ordered state_group_edges table.
logger.info("loading SG state")

# state_groups[sg_id] = { (event_type, state_key): event_id }
state_groups = {}

state_set = set() # the set of event_ids in current state as of last_sg_id
last_sg_id = None # the SG id being accumulated
sg = {} # sg[(type,key)] = event_id. the current stategroup being accumulated (with id last_sg_id)
type_dict = {} # type_dict[evemt_id] = (type,key) for remembering the type of a given event id

sg_id_list = sorted(sg_id_set)
del sg_id_set

batch_size = 100
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

        def handle_last_sg(state_set):
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

            logger.debug(f"len(new_state_set)={len(new_state_set)} len(state_set)={len(state_set)}")

            # todo: parallelise this somehow. it's not even using 1 thread.
            # on M1, it takes 30m for 50,000 state groups in HQ
            mh = MinHash()
            for e in new_state_set:
                mh.update(e.encode('utf8'))
            minhash = mh.hashvalues.astype(np.int64)
            minhash_s32 = ((minhash % (2**32)) - 2**31).astype(np.int32).tolist()
            add_count = len(new_ids)
            gone_count = len(gone_ids)
            #logger.debug(f"calculated minhash {minhash}")

            add_row(last_sg_id, minhash_s32, add_count, gone_count)

            return (new_state_set)

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
