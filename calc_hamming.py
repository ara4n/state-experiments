#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
import numpy as np
from scipy.sparse.csgraph import minimum_spanning_tree
from scipy.sparse import csr_matrix
# from scipy.spatial.distance import pdist, squareform
# from scipy.cluster.hierarchy import linkage, leaves_list
# from psycopg2.extensions import register_adapter, AsIs

# register_adapter(np.int32, AsIs)

# Go through the minhashes table, calculating the hamming distance between all LSH bands
# and then BFS through the MST to order them

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

cursor.execute("SELECT sg_id, lsh_bands FROM minhashes order by sg_id");
sg_id_list = []
sig_list = []
for (sg_id, sig) in cursor.fetchall():
    sg_id_list.append(sg_id)
    sig_list.append(sig)

cursor.execute("UPDATE minhashes SET branch=NULL");

import pprint
# pprint.pp(lsh_bands_list)

sig_len = len(sig_list[0])

def distance(sig1, sig2):
    return sig_len - len(set(sig1) & set(sig2))

def order_sigs(sigs):
    n = len(sigs)
    
    print(f"Ordering {n} sigs using BFS on MST...")
    
    # Build distance matrix
    print("Building distance matrix...")
    distances = np.zeros((n, n))
    for i in range(n):
        for j in range(i+1, n):
            dist = distance(sigs[i], sigs[j])
            distances[i][j] = distances[j][i] = dist
        
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
    
    # Find leaf nodes (degree 1) as potential starting points
    leaves = [i for i in range(n) if len(adj[i]) == 1]
    start_node = leaves[0] if leaves else 0

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
