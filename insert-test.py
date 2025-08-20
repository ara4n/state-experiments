#!/usr/bin/python3

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

nodes = range(101, 116)
branches = [
    # the segment from (105,109] needs to be inserted after node with value 102.
    { "start": 105, "end": 110, "branch": 102},

    # then the segment from (110,114] needs to be inserted after node with value 107
    # as of its new current position in the node list:
    { "start": 110, "end": 115, "branch": 107},
]

nodes = reorder(nodes, branches)
print(nodes)