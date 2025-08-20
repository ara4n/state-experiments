#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
import numpy as np
from scipy.spatial.distance import pdist, squareform
from scipy.cluster.hierarchy import linkage, leaves_list
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int32, AsIs)

# Go through the minhashes table, mapping the LSH bands onto a 1D hilbert curve to group things by proximity.

# ALTER TABLE minhashes ADD COLUMN IF NOT EXISTS ordering BIGINT;

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

cursor.execute("SELECT sg_id, minhash FROM minhashes order by sg_id");
sg_id_list = []
sig_list = []
for (sg_id, sig) in cursor.fetchall():
    sg_id_list.append(sg_id)
    sig_list.append(sig)

cursor.execute("UPDATE minhashes SET branch=NULL");

import pprint
# pprint.pp(lsh_bands_list)

def distance(sig1, sig2):
    return 128 - len(set(sig1) & set(sig2))

def order_sigs(sigs):
    n = len(sigs)
    
    print(f"Ordering {n} sigs using greedy nearest neighbor...")
    
    # Start with greedy nearest neighbor
    ordered = [0]
    remaining = set(range(1, n))
    
    while remaining:
        current_idx = ordered[-1]
        current_sig = sigs[current_idx]
        
        # Find nearest unvisited sig
        min_dist = float('inf')
        nearest = None
        
        for idx in remaining:
            dist = distance(current_sig, sigs[idx])
            if dist < min_dist:
                min_dist = dist
                nearest = idx
        
        ordered.append(nearest)
        remaining.remove(nearest)
        
        # Progress indicator
        if len(ordered) % 1000 == 0:
            print(f"Initial ordering: {len(ordered)}/{n}")
        
    return ordered

# sg_id_list = [ 1,2,3,4,5,6 ]
# lsh_bands_list = [
#     [ 0, 0, 0, 0, 0, 0, 0, 0 ], 
#     [ 0, 0, 0, 0, 0, 0, 0, 1 ], 
#     [ 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF ], 
#     [ 0, 0, 0, 0, 0, 0, 0, 2 ], 
#     [ 0, 0, 0, 0, 0, 0, 0, 3 ], 
#     [ 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF, 0x7FFFFFFF ], 
# ]

ordering = order_sigs(sig_list)

ordered_ids = [ sg_id_list[order] for order in ordering ]

# for order in ordering:
#     if order > 0:
#         print(f"{order}: {sg_id_list[order]}: distance to previous { distance(lsh_bands_list[order-1], lsh_bands_list[order])}")

# pprint.pp(ordering)

# set the new ordering
update_data = list(zip(sg_id_list, ordered_ids))
pprint.pp(update_data)

execute_values(
    cursor,
    "UPDATE minhashes SET ordering = data.o FROM (VALUES %s) AS data(o, sg_id) WHERE minhashes.sg_id = data.sg_id",
    update_data,
    template=None,
    page_size=1000
)
