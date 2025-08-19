#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import tracemalloc
import gc
import sys
import numpy as np
from datasketch import MinHashLSH, MinHash

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

# CREATE TABLE mhstate (
#   start_sg_id bigint not null,
#   end_sg_id bigint,
#   event_id text,
#   room_id text,
#   type text,
#   state_key text,
#   minhash integer[], -- 128 minhash values
#   lsh_bands integer[] -- 16 LSH bands (hashed from the above), so 16 hashes of 8 minhash values
# );
#
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

conn = psycopg2.connect(**DB_CONFIG)
conn.set_session(autocommit=True)

state_table = []
lifetimes = {} # event_id -> ( start_sg, end_sg )

def add_state(sg_id, event_id, event_type, state_key, minhash):
    logger.debug(f"adding {sg_id} {event_id} {event_type} {state_key} {minhash}")
    row = [sg_id, None, event_id, room_id, event_type, state_key, minhash]
    state_table.append(row)
    lifetimes[event_id] = row

def mark_state_as_gone(last_sg_id, event_id):
    logger.debug(f"marking {event_id} as gone in {last_sg_id}")
    row = lifetimes[event_id]
    row[1] = last_sg_id

def dump_state():
    c = conn.cursor()
    execute_values(
        c,
        "INSERT INTO mhstate (start_sg_id, end_sg_id, event_id, room_id, type, state_key, minhash) VALUES %s",
        state_table,
        page_size=1000,
    )

    # FIXME: lots of scope for memoizing obviously
    c.execute("""
    UPDATE mhstate
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

    # we can visualise the LSH bands with:
    # select start_sg_id, end_sg_id, left(event_id,16), left(type,16), left(state_key,32), array(select lpad(to_hex(x), 8, '0') from unnest(lsh_bands) as x) from mhstate order by start_sg_id;
    #
    # 639743464 |            | $Vchdmyj6oQy5tBW | m.room.avatar    |                                  | {757729b2,5c1b99b6,c284b534,ba1f54c3,dd5e47c4,017f95b7,771865f0,3df97fb2,bfc925db,012bc07c,f79f8092,adf2ee03,37823dcf,2b2e75f8,baeedf88,f667e729}
    # 639743466 |            | $TwqYfv-XP2GFNrK | m.room.encryptio |                                  | {757729b2,5c1b99b6,65eab705,2ccb5cf3,6636d836,b7de74a1,771865f0,5fa88652,664e9dcf,954434f0,f79f8092,4c2835c2,b6a3d3c5,3f0499b4,e0c86c7c,338aa1d3}
    # 639743467 |            | $S8Ox54P9v55S8s9 | m.room.guest_acc |                                  | {ae75897b,39d31578,a8a3727f,b1ad3711,0c2a42c1,b7de74a1,409692ac,5fa88652,accc4b73,35674ac0,8c2ad845,bcf4af7d,6280b6f9,3f0499b4,3042cc6a,b8961171}
    # 639743468 |            | $54gyk47h2WkFaW0 | m.room.history_v |                                  | {1e5661bb,3b324e0d,82bba6e7,0168deba,3cf967c3,b7de74a1,e2630023,89757a6b,accc4b73,302ef327,cda9dd48,47526728,2b812e68,b07a8bc7,e87faa43,845b47c1}
    # 639743469 |  639743574 | $HVFH3YP0KDbbZZk | m.room.join_rule |                                  | {926cb62d,709d0ced,ad6c3f65,7903f913,7f0e9d27,96fd7a8c,e2630023,cdd62d8d,accc4b73,1f34b358,6a596cf2,104dfcef,e06450a2,163d6729,e87faa43,3aadee53}
    # 639743471 |            | $dndJwmiZPNAJfyY | m.room.name      |                                  | {247b4e21,0bb6448c,94db344e,7903f913,7f0e9d27,96fd7a8c,c6dfd079,cdd62d8d,e4621f62,6eb5aa00,6a596cf2,089e5bbe,e06450a2,163d6729,e87faa43,e6262fa9}
    # 639743472 |  696859313 | $ml7MiuBd2fStkX7 | m.room.topic     |                                  | {50857a63,0bb6448c,94db344e,7903f913,7f0e9d27,96fd7a8c,c6dfd079,cdd62d8d,dfa09cf6,d1b924ce,6a596cf2,089e5bbe,e06450a2,163d6729,b44d6d96,e6262fa9}
    # 639743574 |            | $hCAKHghXrx1VC4C | m.room.join_rule |                                  | {2fac815e,51533a9b,741f8fc8,5476c46d,d0f0cf6b,f2caa66f,12707c59,89757a6b,dfa09cf6,d1b924ce,cda9dd48,9ed232bb,2b812e68,7a7ea869,a463c5f9,2fd38917}
    #
    # or to spot gaps in the SG DAG:
    #
    # SELECT start_sg_id, end_sg_id, prev_state_group AS prev_sg, LEFT(event_id,16), LEFT(type,16), LEFT(state_key,32),
    # ARRAY(SELECT LPAD(TO_HEX(x), 8, '0') FROM UNNEST(lsh_bands) AS x) 
    # FROM mhstate s
    # LEFT JOIN state_group_edges sge ON sge.state_group=s.start_sg_id
    # ORDER BY start_sg_id;
    
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

def get_state_dict(sg_id):
    if sg_id in state_groups:
        sg = state_groups[sg_id]
        if sg_id in prev_edges:
            prevs = prev_edges[sg_id]
            for prev_id in prevs:
                if prev_id in state_groups:
                    # logger.debug(f"merging: {get_state(prev_id)} with {sg}")
                    sg = get_state_dict(prev_id) | sg

        #             # we can remove older SGs here if this was the only
        #             # place we referred to them, effectively memoizing our
        #             # results.
        #             # on #nvi, this increases our speed by 2x and reduces our peak RAM by 2.5x
        #             logger.debug(f"merging sg {prev_id} into sg {sg_id}")
        #             state_groups[sg_id] = sg

        #             # if (len(next_edges[prev_id]) == 1):
        #             #     logger.debug(f"purging fully merged sg {prev_id}")
        #             #     del state_groups[prev_id]
        #             #     # as an optimisation, we deliberately skip clearing up next_edges and prev_edges as
        #             #     # we know we won't refer to this SG again.
        #             #     # In practice, this seems to buy us very little (but costs a bunch of RAM)
        #             # else:
        #             #     next_edges[prev_id] = [ id for id in next_edges[prev_id] if id != sg_id ]
        #             #     prev_edges[sg_id] = [ id for id in prev_edges[sg_id] if id != prev_id ]

        #             next_edges[prev_id] = [ id for id in next_edges[prev_id] if id != sg_id ]
        #             prev_edges[sg_id] = [ id for id in prev_edges[sg_id] if id != prev_id ]
        #             if (len(next_edges[prev_id]) == 0):
        #                 logger.debug(f"purging fully merged sg {prev_id}")
        #                 del state_groups[prev_id]

        # if sg_id not in next_edges:
        #     logger.debug(f"purging dead-end sg {sg_id}")
        #     del state_groups[sg_id]
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

# XXX: state_set is what we need to consider per-era!
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
            #logger.debug("sg: ", sg)
            #logger.debug("state: ", get_state(last_sg_id))

            new_state_set = set(get_state_dict(last_sg_id).values())
            #logger.debug(f"last_sg_id: {last_sg_id}, state_groups[] = {sg}, new_state_set: {new_state_set}")
            #logger.debug("state_set: ", state_set)
            #logger.debug("new_state_set: ", new_state_set)

            new_ids = new_state_set - state_set
            gone_ids = state_set - new_state_set
            logger.debug(f"new_ids {new_ids}")
            logger.debug(f"gone_ids {gone_ids}")
            for id in new_ids:
                (et, esk) = type_dict[id]
                mh = MinHash()
                for e in new_state_set:
                    mh.update(e.encode('utf8'))
                minhash = mh.hashvalues.astype(np.int64)
                minhash_s32 = ((minhash % (2**32)) - 2**31).astype(np.int32).tolist()
                #logger.debug(f"calculated minhash {minhash}")
                add_state(last_sg_id, id, et, esk, minhash_s32)
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
# logger.debug("state_groups")
# pprint.pp(state_groups)
# logger.debug("prev_edges")
# pprint.pp(prev_edges)
# logger.debug("next_edges")
# pprint.pp(next_edges)

# # Take heap snapshot before dumping
# from pympler import tracker, muppy, summary
#
# logger.debug("Taking heap snapshot...")
# heap = muppy.get_objects()
# sum_stats = summary.summarize(heap)
#
# # Print top memory consumers
# logger.debug("=== HEAP ANALYSIS BEFORE DUMP_STATE ===")
# summary.print_(sum_stats, limit=15)

# finally, dump the state table to the DB.
dump_state()

# then, we can query current state with:
# select * from state where end_sg_id is null;
#
# or for historic state, with:
# select * from state where start_sg_id <= 747138778 and (end_sg_id is null or end_sg_id > 747138778);
#
# or to use the indexes more efficiently:
#
# SELECT event_id FROM matthew_state 
# WHERE room_id='!OGEhHVWSdvArJzumhm:matrix.org' 
# AND start_sg_id <= 960720207 
# AND end_sg_id IS NULL
# UNION ALL
# SELECT event_id FROM matthew_state 
# WHERE room_id='!OGEhHVWSdvArJzumhm:matrix.org' 
# AND start_sg_id <= 960720207 
# AND end_sg_id > 960720207;
#
# (An alternative would be to set end_sg_id as MAX_BIGINT rather than NULL to avoid the partial indices)
#
# This takes ~150ms on matrix.org (with a table with 77M rows in it due to state resets flipflopping state)
# Relative to ~510ms for the recursive query (albeit on a *much* bigger table):
#
# WITH RECURSIVE sgs(state_group) AS (
#     VALUES(960720207::bigint)
#     UNION ALL
#     SELECT prev_state_group FROM matrix.state_group_edges e, sgs s
#     WHERE s.state_group = e.state_group
# )
# SELECT DISTINCT ON (type, state_key)
#     event_id
#     FROM matrix.state_groups_state
#     WHERE state_group IN (
#         SELECT state_group FROM sgs
#     )
#     ORDER BY type, state_key, state_group DESC;


# logger.debug("gc memory", gc.collect())

# current, peak = tracemalloc.get_traced_memory()
# logger.debug(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
# logger.debug(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")
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

