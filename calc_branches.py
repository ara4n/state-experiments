#!/usr/bin/env python3

import psycopg2
# import psycopg2.extensions
import logging
import sys

# Go through the minhashes table, checking for branches whenever the state set jumps

# class LoggingCursor(psycopg2.extensions.cursor):
#     def execute(self, sql, args=None):
#         # print(f"EXECUTING: {sql}")
#         # if args:
#         #     print(f"ARGS: {args}")
#         try:
#             result = super().execute(sql, args)
#             print(f"ACTUAL QUERY: {self.query.decode('utf-8')}")
#             return result
#         except Exception as e:
#             print(f"ERROR: {e}")
#             raise

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

conn = psycopg2.connect("dbname=test") # , cursor_factory=LoggingCursor
conn.set_session(autocommit=True)
cursor = conn.cursor()

cursor.execute("UPDATE minhashes SET branch=NULL");
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
        c.execute("UPDATE minhashes SET branch = %s WHERE sg_id = %s", [branch_point, sg_id])

# propagate through our chosen branches.
cursor.execute("UPDATE minhashes SET branch = sg_id WHERE sg_id = (SELECT MIN(sg_id) FROM minhashes)")

cursor.execute("""
UPDATE minhashes 
SET branch = subq.last_branch
FROM (
    SELECT DISTINCT
        m1.sg_id,
        FIRST_VALUE(m2.branch) OVER (
            PARTITION BY m1.sg_id 
            ORDER BY m2.sg_id DESC
        ) as last_branch
    FROM minhashes m1
    JOIN minhashes m2 ON m2.sg_id <= m1.sg_id AND m2.branch IS NOT NULL
) subq
WHERE minhashes.sg_id = subq.sg_id 
AND minhashes.branch IS NULL;
""")