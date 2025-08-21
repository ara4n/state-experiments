# Synapse state storage experiments

A very hacky bunch of experiments, playing with different algorithms for ordering Matrix state sets
in a temporal table as close together as possible for maximum efficiency.

TL;DR: current pipeline is:
```bash
 ./calc_minhash.py
 ./calc_segmented_tsp.py
 ./calc_state.py
 ```

* compress.py
  * walks synapse's SG tables for a given room_id, turning them into a new temporal table called `state`, which tracks in which SG ID each event_id got added and removed from the current state set.
  * the resulting temporal table is 8436 rows (from 79690 state group state rows, and across 7376 state groups, max room state of 465 state events, for #nvi)
* compress_memoised.py
  * does the same, but shifts most of the processing from SQL to Python and collapses the results of the recursive SGS queries, speeding things up ~100x and saving RAM.  #nvi goes from 30s to 500ms or so, and 6MB of RAM.  Can handle big tables like Matrix HQ (150GB of state group state; roughly 1B rows, 410K SGs, max state 133K events) in an hour or so.
  * However, the algorithm proves not to do well in the face of unstable state, especially when Synapse flipflops storing different SGs in the event of races of lots of state traffic and/or state resets.  This means Matrix HQ only compresses down to 8GB and 77,367,633 rows; it should be much better.
* compress_dag_ordered.py
  * tries to improve compression by ordering the SGs not by ID, but topologically by Kahn (and then by ID, given the DAG is split into ~100 SG chunks).
  * The optimisation doesn't seem to buy much; down to 8214 rows for #nvi - Kahn doesn't consider similarity, after all.
  * For one thing, all the ~100 item chunks don't get ordered with respect to each other, other than chronologically (which they already are)
* compress_minhash.py
  * calculates minhashes & LSH bands for each SG, to visualise when SGs are flipflopping. obsoleted by calc_minhash.py below.
* colorize.py
  * helps visualise LSH bands by colourcoding them on a 24-bit capable terminal

* calc_minhash.py
  * generates minhashes SG by SG, rather than focusing on creating a temporal table, which we can then use to calculate a better ordering based on minhashes
* calc_branches.py
  * goes through the minhashes table, looking for big jumps in current state, and then querying minhashes to find a better ordering by grouping the SGs into 'branches'
  * except this doesn't work very well, as we just end up shifting the big jumps to the *end* of the reordered sequence.
  * in fact, this ends up compressing #nvi to 8382 rows.
* calc_hilbert.py
  * fork of calc_branches, which goes through the minhashes table simply trying to order by proximity on hilbert space based on LSH bands.
  * with 16 bands and hilbert order of 8 then this doesn't work great - 95023 rows :/
  * with 8 bands and hilbert order of 32 then it's 97537 rows
  * with 8 bands and hilbert order of 8 then it's 94808 rows
  * Unintuitively fewer dimensions (by hashing down the LSH bands further) seems to improve locality, but not by much.
  * Perhaps this reflects the fact that the numerical distance between bands is meaningless - only commonality is, so we end up jumping around the curve at random whenver a band changes value, even if the others dimensions are nearby.
* calc_hamming.py
  * simply calculates distance as the number of LSB bands that two SGs have in common, and treat it as the travelling salesman problem
  * minimise it using greedy nearest-neighbour
    * fails entirely for outliers, which get bunched together as ~63 stragglers at the end, causing a whole bunch of flickering which outweighs the benefits elsewhere
    * ends up with 10101 rows when looking at distance between minhashes, and 9877 when looking at distance between lsh_bands
  * another option could be DFS through the MST of the resulting distances.
    * which looks quite good - but by default clusters in ascending order... and then descending again, meaning it uses roughly twice the storage that it should: 15606 rows.
    * so can we change the distance calculation to encourage it to prioritise IDs going up?
  * YES! BFS works well, and gives 8045 rows. Given the theoretical minimum is 7376 + 465 = 7841 (ignoring any state churn at all), this is pretty good!
    * Visually there are still some odd ones out though. Plus, this requires O(N^2) to calculate the distances between all the SG LSH bands
    * It does flap back and forth a bit still; worst-case 30 times on the dataset (down from 46 times)
* calc_segmented_mst.py
  * fork of calc_hamming.py which first segments the SG list based on jumps and branch points, and then applies the MST BFS to the resulting segments, looking at the hamming distance from the end to the start of each segment.
  * Effectively, it's a clustering strategy - a hybrid between calc_branches and calc_hamming
  * this means we only have 76 segments to order, so it's much more efficient than doing all 7280 SGs
  * however, it doesn't seem to work quite as well - compresses to 8796 thanks to some flipflopping.
  * Expanding the cut points to any point in time doesn't help (=> 8869 rows)
  * The problem seems to be that the MST contains lead nodes which end up inserted in a bad order; would be better to exclude them from the MST and then manually slot them in based on distance or even chronology
  * alternatively...
* calc_segmented_tsp.py
  * fork of calc_segmented_mst.py which instead treats it as the travelling salesperson problem between clusters, given that's what we're actually doing here, and given we only have 76 rows to play with.
  * Using elkai, this returns 7901 and only takes 900ms for 76 segments - so our best yet.
* alternatively, we could try calculating the optimal branching (aka minimum weight spanning arborescence), which is effectively the MST of the directed graph and BFS it.
  * This is what calc_branches.py was clumsily converging on - however, it would suffer the same problem of the extremities of the branches not being aligned. TSP should be better.
* calc_state.py
  * fork of compress_dag_ordered.py which loads the state in the order from calc_branches/hilbert/hamming/segmented_mst/segmented_tsp and compresses it.

Next steps:
 * consider using the state DAG to get better similarity for adjacent temporal table rows
