#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys

# Go through the minhashes table, checking for branches whenever the state set jumps
# and defining a new ordering based on that.

class LoggingCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        # print(f"EXECUTING: {sql}")
        # if args:
        #     print(f"ARGS: {args}")
        try:
            result = super().execute(sql, args)
            print(f"ACTUAL QUERY: {self.query.decode('utf-8')}")
            return result
        except Exception as e:
            print(f"ERROR: {e}")
            raise

# ALTER TABLE minhashes ADD COLUMN IF NOT EXISTS ordering BIGINT;
# ALTER TABLE minhashes ADD COLUMN IF NOT EXISTS branch BIGINT;

# CREATE OR REPLACE FUNCTION jaccard_similarity(sig1 integer[], sig2 integer[])
# RETURNS float AS $$
# SELECT 
#     -- Use generate_subscripts to iterate through array indices
#     (SELECT COUNT(*) 
#      FROM generate_subscripts(sig1, 1) AS i 
#      WHERE sig1[i] = sig2[i]
#     )::float / array_length(sig1, 1);
# $$ LANGUAGE sql IMMUTABLE;

logger = logging.getLogger()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'

conn = psycopg2.connect("dbname=test") #, cursor_factory=LoggingCursor)
conn.set_session(autocommit=True)
cursor = conn.cursor()

cursor.execute("UPDATE minhashes SET branch=NULL");
branches = []

cursor.execute("SELECT sg_id, minhash, lsh_bands FROM minhashes WHERE add_count + gone_count > 5 ORDER BY sg_id");
for (sg_id, minhash, lsh_bands) in cursor.fetchall():
    logger.info(f"checking jump at sg_id: {sg_id}")
    c = conn.cursor()
    # XXX: check this actually does an efficient query
    c.execute("""
        WITH query_bands AS (
            SELECT %s AS bands
        )
        SELECT sg_id FROM minhashes, query_bands
        WHERE (
            SELECT COUNT(*)
            FROM unnest(lsh_bands) AS band
            WHERE band = ANY(query_bands.bands)
        ) >= 8
        -- WHERE lsh_bands && query_bands.bands
        AND sg_id < %s
        ORDER BY jaccard_similarity(minhash, %s) DESC, sg_id DESC
        LIMIT 1;
    """, [lsh_bands, sg_id, minhash])
    row = c.fetchone()
    logger.info(f"found branch point {row}")
    if row is not None:
        branch_point = row[0]
        branches.append({ "start": sg_id, "branch": branch_point })
        #c.execute("UPDATE minhashes SET branch = %s WHERE sg_id = %s", [branch_point, sg_id])

cursor.execute("SELECT sg_id FROM minhashes ORDER BY sg_id");
nodes = list(row[0] for row in cursor.fetchall())

# figure out our branches
last_branch = None
for branch in branches:
    if last_branch is not None:
        last_branch['end'] = branch['start']
        cursor.execute("UPDATE minhashes SET branch=%s WHERE sg_id>=%s AND sg_id<%s", [
            last_branch['branch'],
            last_branch['start'],
            last_branch['end'],
        ])
    last_branch = branch
branches[-1]['end'] = nodes[-1] + 1 # XXX: we make up a hypothetical branch ID here.
cursor.execute("UPDATE minhashes SET branch=%s WHERE sg_id>=%s AND sg_id<%s", [
    last_branch['branch'],
    last_branch['start'],
    last_branch['end'],
])
cursor.execute("UPDATE minhashes SET branch=%s WHERE sg_id>=%s AND sg_id<%s", [
    nodes[0],
    0,
    branches[0]['start'],
])

import pprint
pprint.pp(branches)

# takes a list of nodes, and a list of branch-points.
# each branch-point is a dict which defines a segment of the original list with start (incl) & end (excl) node IDs (not offsets)
# which need to be inserted after a given node value in the node list.  This has to be done iteratively, applying to the
# current value of the node list.
# It returns the reordered node list.
# For instance, in the sample data below, it should return:
# [ 101, 102, 105, 106, 107, 110, 111, 112, 113, 114, 108, 109, 103, 104, 115 ]
def reorder(nodes, branches):
    result = list(nodes)
    
    for branch in branches:
        start_val = branch["start"]
        end_val = branch["end"]
        insert_after_val = branch["branch"]
        
        segment = []
        indices_to_remove = []
        
        for i, val in enumerate(result):
            if start_val <= val < end_val:
                segment.append(val)
                indices_to_remove.append(i)
        
        for i in reversed(indices_to_remove):
            del result[i]
        
        insert_after_idx = result.index(insert_after_val)
        
        for i, val in enumerate(segment):
            result.insert(insert_after_idx + 1 + i, val)
    
    return result

pprint.pp(nodes)
ordered_nodes = reorder(nodes, branches)
pprint.pp(ordered_nodes)


# set the new ordering
update_data = list(zip(nodes, ordered_nodes))
execute_values(
    cursor,
    "UPDATE minhashes SET ordering = data.o FROM (VALUES %s) AS data(o, sg_id) WHERE minhashes.sg_id = data.sg_id",
    update_data,
    template=None,
    page_size=1000
)

# propagate through our chosen branches.

# # set the initial branch
# cursor.execute("UPDATE minhashes SET branch = sg_id WHERE sg_id = (SELECT MIN(sg_id) FROM minhashes)")

# cursor.execute("""
# UPDATE minhashes 
# SET branch = subq.last_branch
# FROM (
#     SELECT DISTINCT
#         m1.ordering,
#         FIRST_VALUE(m2.branch) OVER (
#             PARTITION BY m1.ordering
#             ORDER BY m2.ordering DESC
#         ) as last_branch
#     FROM minhashes m1
#     JOIN minhashes m2 ON m2.ordering <= m1.ordering AND m2.branch IS NOT NULL
# ) subq
# WHERE minhashes.ordering = subq.ordering
# AND minhashes.branch IS NULL;
# """)
