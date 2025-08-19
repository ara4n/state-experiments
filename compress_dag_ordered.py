#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import tracemalloc
import gc
import sys
#import numpy as np
#from datasketch import MinHashLSH, MinHash
from collections import defaultdict, deque

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

# from claude
def find_chunks(nodes, edges):
    """
    Find disconnected chunks in the DAG.
    
    Args:
        nodes: Set or list of node IDs
        edges: Dict mapping source nodes to destination(s)
    
    Returns:
        List of sets, where each set contains nodes in one chunk
    """
    # Build undirected graph for finding connected components
    graph = defaultdict(set)
    
    for origin, destinations in edges.items():
        if isinstance(destinations, (list, tuple, set)):
            for destination in destinations:
                graph[origin].add(destination)
                graph[destination].add(origin)  # Make undirected
        else:
            graph[origin].add(destinations)
            graph[destinations].add(origin)  # Make undirected
    
    visited = set()
    chunks = []
    
    for node in nodes:
        if node not in visited:
            # BFS to find connected component
            chunk = set()
            queue = deque([node])
            
            while queue:
                current = queue.popleft()
                if current not in visited:
                    visited.add(current)
                    chunk.add(current)
                    
                    # Add unvisited neighbors
                    for neighbor in graph[current]:
                        if neighbor not in visited:
                            queue.append(neighbor)
            
            chunks.append(chunk)
    
    return chunks

def order_chunks_chronologically(chunks):
    """
    Order chunks chronologically based on lexicographic ordering of node IDs.
    
    Args:
        chunks: List of sets, where each set contains nodes in one chunk
    
    Returns:
        List of chunks ordered chronologically (earliest first)
    """
    # For each chunk, find the lexicographically smallest node ID
    chunk_min_ids = []
    for chunk in chunks:
        min_id = min(chunk)  # Lexicographically smallest ID in chunk
        chunk_min_ids.append((min_id, chunk))
    
    # Sort by minimum ID
    chunk_min_ids.sort(key=lambda x: x[0])
    
    return [chunk for _, chunk in chunk_min_ids]

def topological_sort_chunked(nodes, edges):
    """
    Topologically sort nodes, ordering chunks chronologically first,
    then topologically within each chunk.
    
    Args:
        nodes: Set or list of node IDs (strings)
        edges: Dict where key is origin node ID, value is a list of destination node IDs
    
    Returns:
        List of nodes in topological order with chunks ordered chronologically
    
    Raises:
        ValueError: If any chunk contains a cycle
    """
    # Find disconnected chunks
    chunks = find_chunks(nodes, edges)
    
    # Order chunks chronologically
    ordered_chunks = order_chunks_chronologically(chunks)
    
    result = []
    
    # Process each chunk in chronological order
    for chunk in ordered_chunks:
        # Extract edges within this chunk
        chunk_edges = {}
        for origin, destinations in edges.items():
            if origin in chunk:
                # Filter destinations to only include nodes in this chunk
                chunk_destinations = [dest for dest in destinations if dest in chunk]
                if chunk_destinations:
                    chunk_edges[origin] = chunk_destinations
        
        # Topologically sort within this chunk
        chunk_sorted = topological_sort(chunk, chunk_edges)
        result.extend(chunk_sorted)
    
    return result

def topological_sort(nodes, edges):
    """
    Topologically sort nodes according to DAG edges using Kahn's algorithm.
    
    Args:
        nodes: Set or list of node IDs (strings)
        edges: Dict where key is origin node ID, value is a list of destination node IDs
    
    Returns:
        List of nodes in topological order
    
    Raises:
        ValueError: If the graph contains a cycle
    """
    # Build adjacency list and calculate in-degrees
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    
    # Initialize in-degree for all nodes
    for node in nodes:
        in_degree[node] = 0
    
    # Build graph and calculate in-degrees
    for origin, destinations in edges.items():
        for destination in destinations:
            graph[origin].append(destination)
            in_degree[destination] += 1
    
    # Find all nodes with no incoming edges
    queue = deque([node for node in nodes if in_degree[node] == 0])
    result = []
    
    # Process nodes with no incoming edges
    while queue:
        current = queue.popleft()
        result.append(current)
        
        # Remove current node and update in-degrees of neighbors
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Check for cycles
    if len(result) != len(nodes):
        raise ValueError("Graph contains a cycle - topological sort not possible")
    
    return result


conn = psycopg2.connect(**DB_CONFIG)
conn.set_session(autocommit=True)

state_table = []
lifetimes = {} # event_id -> ( start_sg, end_sg )

def add_state(index, sg_id, event_id, event_type, state_key):
    logger.debug(f"adding {index} {sg_id} {event_id} {event_type} {state_key}")
    row = [index, None, sg_id, None, event_id, room_id, event_type, state_key]
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
        "INSERT INTO state (start_index, end_index, start_sg_id, end_sg_id, event_id, room_id, type, state_key) VALUES %s",
        state_table,
        page_size=1000,
    )
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

# the idea is that we improve compression by reordering the SGs topologically via Kahn's alg
# (while also sorting the chronologically in the event that they are chunked - i.e. in each batch of 100 SGs)
#
# This results in 8214 rows in the state table for #NVI, out of 7279 SGs
# but with only a handful of reordered chunks:
#
# test=# select * from (select start_index, start_sg_id, start_sg_id - lag(start_sg_id) over (order by start_index) as sg_diff from state order by start_index) diffs where diffs.sg_diff<0;
#  start_index | start_sg_id | sg_diff 
# -------------+-------------+---------
#         3899 |   781243429 |    -200
#         5999 |   933172116 |    -415
#         6999 |  1040664232 |    -528
#         7099 |  1040664872 |    -659
#         7199 |  1040665367 | -160835
#
# In other words, it looks like the SGs are already Kahn-ordered within each chunk, which kinda makes sense.
#
# This compares with the naive memoised approach as follows:

sg_id_list = topological_sort_chunked(sg_id_set, next_edges)
del sg_id_set

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
                # mh = MinHash()
                # for e in new_state_set:
                #     mh.update(e.encode('utf8'))
                # minhash = mh.hashvalues.astype(np.int64)
                # minhash_s32 = ((minhash % (2**32)) - 2**31).astype(np.int32).tolist()
                #logger.debug(f"calculated minhash {minhash}")
                add_state(index, last_sg_id, id, et, esk)
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

