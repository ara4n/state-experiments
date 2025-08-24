#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
import pprint
from collections import deque
import numpy as np
from scipy.sparse.csgraph import minimum_spanning_tree
from scipy.sparse import csr_matrix

# Go through the minhashes table, segmenting into regions where the
# add_count and gone_count aren't too big.
# Then, figure out similarity for both start *and* end of each segment
# based on querying minhash similarity.
# Then, split those segments on both src & dest
# Then use BFS through MST to linearise these segments.

# ALTER TABLE minhashes ADD COLUMN IF NOT EXISTS ordering BIGINT;

logger = logging.getLogger()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# CREATE OR REPLACE FUNCTION jaccard_similarity(sig1 integer[], sig2 integer[])
# RETURNS float AS $$
# SELECT 
#     -- Use generate_subscripts to iterate through array indices
#     (SELECT COUNT(*) 
#      FROM generate_subscripts(sig1, 1) AS i 
#      WHERE sig1[i] = sig2[i]
#     )::float / array_length(sig1, 1);
# $$ LANGUAGE sql IMMUTABLE;

# room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'
room_id = '!OGEhHVWSdvArJzumhm:matrix.org'

conn = psycopg2.connect("dbname=test") #, cursor_factory=LoggingCursor)
conn.set_session(autocommit=True)
cursor = conn.cursor()

cursor.execute("SELECT sg_id FROM minhashes WHERE room_id=%s ORDER BY sg_id", [room_id])
sg_id_list = [ row[0] for row in cursor.fetchall() ]

print("sg_id_list")
print(' '.join(f'{id:10d}' for id in sg_id_list))

lsh_bands = {} # sg_id => []

# we partition into sections whenever there is a jump:

section_starts = []
section_ends = []
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes WHERE room_id=%s order by sg_id limit 1", [room_id])
row = cursor.fetchone()
section_starts.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
lsh_bands[row[0]] = row[1]
ends = []
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where add_count + gone_count > 10 and room_id=%s order by sg_id", [room_id])
for row in cursor.fetchall():
    section_starts.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
    ends.append( sg_id_list[sg_id_list.index(row[0]) - 1] )
    lsh_bands[row[0]] = row[1]
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where room_id = %s and sg_id = any(%s) order by sg_id", [room_id, ends])
for row in cursor.fetchall():
    section_ends.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
    lsh_bands[row[0]] = row[1]
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where room_id = %s order by sg_id desc limit 1", [room_id])
row = cursor.fetchone()
section_ends.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
lsh_bands[row[0]] = row[1]

sections = []
for i, b in enumerate(section_starts):
    sections.append({
        # inclusive range
        'start': section_starts[i],
        'end': section_ends[i]
    })

for section in sections:
    print(f"section { section['start']['sg_id'] } -> { section['end']['sg_id'] }")

# cut points of nodes we cut before or after - due to branchpoints
cut_after = set()  # we cut after the src of links from the past
cut_before = set() # we cut before the dest of links to the future

# find the branchpoints where these segments ideally belong from
# in terms of minhash proximity
for i, section in enumerate(sections):
    c = conn.cursor()
    search_fail = 0
    if i > 0:
        # closest start point - looking only into the past:
        # XXX: if we only look into the past for previous branchpoints and future for future branchpoints
        # then we can end up with finding none and forming islands, which inevitably cause ugly jumps in both TSP & MST.
        # in practice if we search the whole space however we end up with worse results though.
        start = section['start']
        # XXX: check this actually does an efficient query
        c.execute("""
            WITH query_bands AS (
                SELECT %s AS bands
            )
            SELECT sg_id, lsh_bands FROM minhashes, query_bands
            -- WHERE (
            --    SELECT COUNT(*)
            --    FROM unnest(lsh_bands) AS band
            --    WHERE band = ANY(query_bands.bands)
            -- ) >= 8
            WHERE lsh_bands && query_bands.bands
            AND sg_id < %s
            AND room_id = %s
            ORDER BY jaccard_similarity(minhash, %s) DESC, sg_id DESC
            LIMIT 1;
        """, [
            start['lsh_bands'],
            start['sg_id'],
            room_id,
            start['minhash'],
        ])
        row = c.fetchone()
        if row is not None:
            logger.info(f"found start branch point {row[0]} for { start['sg_id'] }")
            lsh_bands[row[0]] = row[1]
            cut_after.add(row[0])
        else:
            logger.info(f"failed to find start branch point for { start['sg_id'] }")
            search_fail += 1

    if i < len(section_starts) - 1:
        # closest end point - currently looking only into the future, to avoid risk of loops
        end = section['end']
        c.execute("""
            WITH query_bands AS (
                SELECT %s AS bands
            )
            SELECT sg_id, lsh_bands FROM minhashes, query_bands
            -- WHERE (
            --    SELECT COUNT(*)
            --    FROM unnest(lsh_bands) AS band
            --    WHERE band = ANY(query_bands.bands)
            --) >= 8
            WHERE lsh_bands && query_bands.bands
            AND sg_id > %s
            AND room_id = %s
            ORDER BY jaccard_similarity(minhash, %s) DESC, sg_id ASC
            LIMIT 1;
        """, [
            end['lsh_bands'],
            end['sg_id'],
            room_id,
            end['minhash'],
        ])
        row = c.fetchone()
        if row is not None:
            logger.info(f"found end   branch point {row[0]} for { end['sg_id'] }")
            lsh_bands[row[0]] = row[1]
            cut_before.add(row[0])
        else:
            logger.info(f"failed to find end branch point for { end['sg_id'] }")
            search_fail += 1

    if search_fail == 2:
        logger.warning(f"Failed to find either start or end for section { start['sg_id'] }->{ end['sg_id'] }")

# grab the LSH bands for SGs on the other side of cut boundaries
other_sgs = set()
for cut in cut_before:
    other_sgs.add( sg_id_list[sg_id_list.index(cut) - 1] )
for cut in cut_after:
    i = sg_id_list.index(cut) + 1
    if i < len(sg_id_list):
        other_sgs.add( sg_id_list[i] )
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where room_id = %s and sg_id = any(%s) order by sg_id", [room_id, list(other_sgs)])
for row in cursor.fetchall():
    lsh_bands[row[0]] = row[1]

# check we have all the LSH Bands
for sg_id in sorted(lsh_bands.keys()):
    print(f"lsh {sg_id:10d}: { ','.join(f'{(x & 0xFFFFFFFF):08x}' for x in lsh_bands[sg_id]) }")

# turn all the cut_befores into cut_afters to make it easier to cut up the sections
for cut in cut_before:
    cut_after.add( sg_id_list[sg_id_list.index(cut) - 1] )

# add all the jump points to cut_afters too to make it easier to cut things up
# as we're going to walk the full SG list again
for section in sections:
    cut_after.add( section['end']['sg_id'] )

for cut in sorted(list(cut_after)):
    print(f"cut_after { cut }")

# split the sg_id_list into segments based on the cut_after points we've now found.

# [ {
#   'ids': [ sg_ids ],
# } ]
segments = []
first_segment = segment = { 'ids': [] }
for i, sg_id in enumerate(sg_id_list):
    segment['ids'].append(sg_id)
    if sg_id in cut_after:
        segments.append(segment)
        segment = { 'ids': [] }
#segments.append(segment)

for segment in segments:
    print(f"segment { segment['ids'][0] } -> { segment['ids'][-1] }")

def dumpdot(adj):
    dot = open("graph.dot", "w")
    print("digraph G {", file=dot)
    print("  rankdir=LR", file=dot)
    for i, segment in enumerate(segments):        
        print(f"  { i } [label=\"{i}:{ segment['ids'][0] }->{ segment['ids'][-1] } ({len(segment['ids'])})\"]", file=dot)
    for i, dests in enumerate(adj):
        for dest in dests:
            if i < dest:
                print(f"  { i } -> { dest } [label=\"{ distance(segments[i], segments[dest]) }\"]", file=dot)
            # else:
            #     print(f"  { i } -> { dest } [label=\"{ distance(segments[i], segments[dest]) }\",style=\"dotted\"]", file=dot)
    print("}", file=dot)
    dot.close()

def distance(seg1, seg2):
    if seg1 == seg2:
        return 0
    lsh_end = lsh_bands[seg1['ids'][-1]]
    lsh_start = lsh_bands[seg2['ids'][0]]
    return 16 - len(set(lsh_end) & set(lsh_start))

def order_segs(segs):
    n = len(segs)
    
    print(f"Ordering {n} segs using BFS on MST...")
    
    # Build distance matrix
    print("Building distance matrix...")
    distances = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            # we allow high->low edges given the order may be shuffled.
            # however, given distance is no longer symmetrical, we have
            # to populate the whole matrix.
            dist = distance(segs[i], segs[j])
            # XXX: for MST, being undirected, the minimum of the two distance is used apparently
            # which is going to give a weird outcome
            distances[i][j] = dist
        
        if (i + 1) % 1000 == 0:
            print(f"Distances calculated: {i + 1}/{n}")
    
    # Find MST
    print("Building minimum spanning tree...")
    
    mst = minimum_spanning_tree(csr_matrix(distances))
    
    # Convert to adjacency list
    print("Converting MST to adjacency list...")
    adj = [[] for _ in range(n)]
    mst_coo = mst.tocoo()
    for i, j in zip(mst_coo.row, mst_coo.col):
        adj[i].append(j)
        adj[j].append(i)
    
    dumpdot(adj)

    # Find leaf nodes (degree 1) as potential starting points
    leaves = [i for i in range(n) if len(adj[i]) == 1]
    start_node = leaves[0] if leaves else 0

    print(f"leaves: { leaves }")

    print(f"Starting BFS from node {start_node}")
    
    # BFS traversal using a queue
    from collections import deque
    visited = [False] * n
    ordered = []
    queue = deque([start_node])
    
    while queue:
        node = queue.popleft()
        
        if visited[node]:
            continue
            
        visited[node] = True
        ordered.append(node)
        
        # Add neighbors to queue in order of distance (closest first)
        neighbors = [(distances[node][neighbor], neighbor) for neighbor in adj[node] if not visited[neighbor]]
        neighbors.sort()  # Sort by distance, closest first
        
        for _, neighbor in neighbors:
            if not visited[neighbor]:
                queue.append(neighbor)
    
    print(f"BFS completed, ordered {len(ordered)} nodes")
    return ordered

segment_ordering = order_segs(segments)
for i, id in enumerate(segment_ordering):
    this_seg = segments[id]
    if i > 0:
        prev_seg = segments[segment_ordering[i - 1]]
    else:
        prev_seg = segments[id]
    print(f"ordered_segment index { id } { segments[id]['ids'][0] } -> { segments[id]['ids'][-1] } dist from prev: { distance(prev_seg, this_seg) }")

ordered_ids = []
for id in segment_ordering:
    ordered_ids.extend(segments[id]['ids'])

print("ordered_ids")
print(' '.join(f'{id:10d}' for id in ordered_ids))

print(f"length of ordered_ids = {len(ordered_ids)}")
if len(ordered_ids) != len(sg_id_list):
    logger.fatal("we've lost SGs")
    sys.exit(1)

# set the new ordering
update_data = list(zip(sg_id_list, ordered_ids))
# pprint.pp(update_data)

execute_values(
    cursor,
    "UPDATE minhashes SET ordering = data.o FROM (VALUES %s) AS data(o, sg_id) WHERE minhashes.sg_id = data.sg_id",
    update_data,
    template=None,
    page_size=1000
)
