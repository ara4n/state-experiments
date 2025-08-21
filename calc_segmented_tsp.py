#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
import pprint
from collections import deque
import numpy as np
import elkai

# Go through the minhashes table, segmenting into regions where the
# add_count and gone_count aren't too big.
# Then, figure out similarity for both start *and* end of each segment
# based on querying minhash similarity.
# Then, split those segments on both src & dest
# Then solve the travelling salesperson problem to linearise these segments.

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

room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'

conn = psycopg2.connect("dbname=test") #, cursor_factory=LoggingCursor)
conn.set_session(autocommit=True)
cursor = conn.cursor()

cursor.execute("SELECT sg_id FROM minhashes order by sg_id")
sg_id_list = [ row[0] for row in cursor.fetchall() ]

print("sg_id_list")
print(' '.join(f'{id:10d}' for id in sg_id_list))

lsh_bands = {} # sg_id => []

# we partition into sections whenever there is a jump:

section_starts = []
section_ends = []
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes order by sg_id limit 1")
row = cursor.fetchone()
section_starts.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
lsh_bands[row[0]] = row[1]
ends = []
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where add_count + gone_count > 10 order by sg_id")
for row in cursor.fetchall():
    section_starts.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
    ends.append( sg_id_list[sg_id_list.index(row[0]) - 1] )
    lsh_bands[row[0]] = row[1]
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where sg_id = any(%s) order by sg_id", [ends])
for row in cursor.fetchall():
    section_ends.append( { "sg_id": row[0], "lsh_bands": row[1], "minhash": row[2] } )
    lsh_bands[row[0]] = row[1]
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes order by sg_id desc limit 1")
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
    if i > 0:
        # closest start point - looking only into the past:
        start = section['start']
        # XXX: check this actually does an efficient query
        c.execute("""
            WITH query_bands AS (
                SELECT %s AS bands
            )
            SELECT sg_id, lsh_bands FROM minhashes, query_bands
            WHERE (
                SELECT COUNT(*)
                FROM unnest(lsh_bands) AS band
                WHERE band = ANY(query_bands.bands)
            ) >= 8
            -- WHERE lsh_bands && query_bands.bands
            AND sg_id < %s
            ORDER BY jaccard_similarity(minhash, %s) DESC, sg_id DESC
            LIMIT 1;
        """, [ start['lsh_bands'], start['sg_id'], start['minhash'] ])
        row = c.fetchone()
        if row is not None:
            logger.info(f"found start branch point {row[0]} for { start['sg_id'] }")
            lsh_bands[row[0]] = row[1]
            cut_after.add(row[0])
        else:
            logger.info(f"failed to find start branch point for { start['sg_id'] }")

    if i < len(section_starts) - 1:
        # closest end point - currently looking only into the future, to avoid risk of loops
        end = section['end']
        c.execute("""
            WITH query_bands AS (
                SELECT %s AS bands
            )
            SELECT sg_id, lsh_bands FROM minhashes, query_bands
            WHERE (
                SELECT COUNT(*)
                FROM unnest(lsh_bands) AS band
                WHERE band = ANY(query_bands.bands)
            ) >= 8
            -- WHERE lsh_bands && query_bands.bands
            AND sg_id > %s
            ORDER BY jaccard_similarity(minhash, %s) DESC, sg_id ASC
            LIMIT 1;
        """, [ end['lsh_bands'], end['sg_id'], end['minhash'] ])
        row = c.fetchone()
        if row is not None:
            logger.info(f"found end   branch point {row[0]} for { end['sg_id'] }")
            lsh_bands[row[0]] = row[1]
            cut_before.add(row[0])
        else:
            logger.info(f"failed to find end branch point for { end['sg_id'] }")

# grab the LSH bands for SGs on the other side of cut boundaries
other_sgs = set()
for cut in cut_before:
    other_sgs.add( sg_id_list[sg_id_list.index(cut) - 1] )
for cut in cut_after:
    other_sgs.add( sg_id_list[sg_id_list.index(cut) + 1] )
cursor.execute("SELECT sg_id, lsh_bands, minhash FROM minhashes where sg_id = any(%s) order by sg_id", [list(other_sgs)])
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

for i, segment in enumerate(segments):
    print(f"segment #{ i } { segment['ids'][0] } -> { segment['ids'][-1] }")

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
    
    print(f"Ordering {n} segs using TSP...")
    
    # Build distance matrix
    print("Building distance matrix...")
    distances = np.zeros((n, n), dtype=int)
    for i in range(n): # row
        for j in range(n): # column
            dist = distance(segs[i], segs[j])
            distances[i][j] = dist
        
        if (i + 1) % 1000 == 0:
            print(f"Distances calculated: {i + 1}/{n}")

    print("  |" + " ".join(f'{i:2d}' for i in range(n)))
    print("---" * (n + 1))
    for i in range(n):
        print(f"{i:2d}|" + " ".join(f'{d:2d}' for d in distances[i]))

    tour = elkai.solve_int_matrix(distances)
    return tour

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
