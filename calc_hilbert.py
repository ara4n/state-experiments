#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
from hilbertcurve.hilbertcurve import HilbertCurve

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

cursor.execute("SELECT sg_id, lsh_bands FROM minhashes");
sg_id_list = []
lsh_bands_list = []
for (sg_id, lsh_bands) in cursor.fetchall():
    sg_id_list.append(sg_id)
    lsh_bands_list.append(lsh_bands)

cursor.execute("UPDATE minhashes SET branch=NULL");

import pprint
# pprint.pp(lsh_bands_list)

# from claude:
def map_lsh_bands_to_hilbert(lsh_bands):
    """
    Map LSH buckets to Hilbert curve positions
    """
    dimensions=16 # to match number of LSH bands
    hilbert_order=8 # 256 per dimension
    # Might harm locality.
    # means that the distance is measured as:
    # 2 ^ 8 ^ 16 = 128-bit number, which we'll have to right-shift back into a 63-bit.

    # Claude recommended:
    # Dimensions = 4:

    # Groups your 16 LSH bands into 4 coordinates (4 bands per coordinate)
    # Keeps the dimensionality manageable for the Hilbert curve
    # Higher dimensions can actually hurt locality preservation in Hilbert curves

    # Hilbert Order = 8:

    # Gives you 256^4 = 4.3 billion possible positions
    # Much larger than your 400k records, so plenty of resolution
    # Not so large that you lose locality due to sparse distribution
    
    # Initialize Hilbert curve
    hilbert_curve = HilbertCurve(hilbert_order, dimensions)
    bands_per_dim = len(lsh_bands[0]) // dimensions

    results = []
    for bands in lsh_bands:
        # coords = []
        
        # for dim in range(dimensions):
        #     start_idx = dim * bands_per_dim
        #     end_idx = start_idx + bands_per_dim
            
        #     # Combine 4 band hashes into one coordinate
        #     combined_hash = 0
        #     for i in range(start_idx, end_idx):
        #         combined_hash ^= hash(str(bands[i]))  # XOR the hashes together
            
        #     # Map to coordinate space [0, 2^hilbert_order)
        #     coord = abs(combined_hash) % (2**hilbert_order)
        #     coords.append(coord)

        coords = [ band % (2 ** hilbert_order) for band in bands ]
        
        hilbert_distance = hilbert_curve.distance_from_point(coords)
        results.append(hilbert_distance >> 65)
    
    return results

def minhash_to_hilbert_direct(minhash_signatures, signature_length=128, dimensions=8):
    """
    Map minhash signatures directly to Hilbert curve
    """
    hilbert_order = 8  # 2^8 = 256 positions per dimension
    hilbert_curve = HilbertCurve(hilbert_order, dimensions)
    
    results = []
    for signature in minhash_signatures:
        # Group signature values into coordinates
        coords_per_dim = signature_length // dimensions
        coords = []
        
        for i in range(dimensions):
            start_idx = i * coords_per_dim
            end_idx = start_idx + coords_per_dim
            # Combine multiple signature values into one coordinate
            coord_val = sum(signature[start_idx:end_idx]) % (2**hilbert_order)
            coords.append(coord_val)
        
        hilbert_distance = hilbert_curve.distance_from_point(coords)
        results.append(hilbert_distance >> 1)
    
    return results

hilbert_distances = map_lsh_bands_to_hilbert(lsh_bands_list)

# set the new ordering
update_data = list(zip(hilbert_distances, sg_id_list))
pprint.pp(update_data)

execute_values(
    cursor,
    "UPDATE minhashes SET ordering = data.o FROM (VALUES %s) AS data(o, sg_id) WHERE minhashes.sg_id = data.sg_id",
    update_data,
    template=None,
    page_size=1000
)
