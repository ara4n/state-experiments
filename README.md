# Synapse state storage experiments

A very hacky bunch of experiments, playing with different algorithms for ordering Matrix state sets
in a temporal table as close together as possible for maximum efficiency.

This is effectively a series of python notebooks, not really intended for anyone but me.

TL;DR: current pipeline is:
```bash
 ./calc_minhash.py
 ./calc_segmented_tsp.py
 ./calc_state.py
 ```

* compress.py
  * walks synapse's SG tables for a given room_id, turning them into a new temporal table called `state`, which tracks in which SG ID each event_id got added and removed from the current state set.
  * the resulting temporal table is 8436 rows (from 79,690 state group state rows, and across 7376 state groups, total state events 7068, max room state of 465 state events, for #nvi)
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
  * YES! BFS on the MST works well, and gives 8045 rows. Given the theoretical minimum is 7376 + 465 = 7841 (ignoring any state churn at all), this is pretty good!
    * Visually there are still some odd ones out though. Plus, this requires O(N^2) to calculate the distances between all the SG LSH bands
    * It does flap back and forth a bit still; worst-case 30 times on the dataset (down from 46 times)
* calc_segmented_mst.py
  * fork of calc_hamming.py which first segments the SG list based on jumps and branch points, and then applies the MST BFS to the resulting segments, looking at the hamming distance from the end to the start of each segment.
  * Effectively, it's a clustering strategy - a hybrid between calc_branches and calc_hamming
  * this means we only have 76 segments to order, so it's much more efficient than doing all 7280 SGs
  * however, it doesn't seem to work quite as well - compresses to 8796 thanks to some flipflopping, or 8483 after some bugfixes.
  * Expanding the cut points to any point in time doesn't help (=> 8869 rows, or 8746 after bugfixes)
  * The problem seems to be that the MST contains lead nodes which end up inserted in a bad order; would be better to exclude them from the MST and then manually slot them in based on distance or even chronology
  * on the first 85K SGs in HQ, this returns 357K state table rows (having expanded minhash search for branchpoints to the whole table to avoid islands: 2601 segments), or 382K (looking just to past & future branchpoints; 2400 segments)
* calc_segmented_msa.py
  * alternatively, we could try calculating the optimal branching (aka minimum weight spanning arborescence), which is effectively the MST of the directed graph and BFS it.
  * This is what calc_branches.py was clumsily converging on - however, it would suffer the same problem of the extremities of the branches not being aligned. TSP should be better.
  * Indeed, BFS on the MSA provides 8300 row in the state table, so better than MST, but not much better than our original calc_branches.
* calc_segmented_tsp.py
  * fork of calc_segmented_mst.py which instead treats it as the travelling salesperson problem between clusters, given that's what we're actually doing here, and given we only have 76 rows to play with.
  * Using elkai, this returns 7901 and only takes 900ms for 76 segments - so our best yet.
  * For the first 85K SGs in HQ, this returns 296K state table rows (out of 16,921,672 state group state entries; 78,493 total events; 2391 segments) - which isn't too bad, even though 296K feels high, given 296K / 16.9M is 1.7%.
     * However, something feels wrong - querying SG 599996791 returns bogus values
     * Also, calc_state ended up finding loads of trailing SGs like 397753848 which it should already have come across... but didn't, or has subsequently forgotten.
     * this is because we incorrectly specified an 8-band minimum overlap for LSH bands, and we ended up with fully disconnected islands as a result. Reducing to 1 band should avoid this.
    * Trying again with fewer disconnected islands (1-band overlap, but searching only past-for-prev and future-for-next SGs after jumps), we get 273K state rows: not a great improvement.
     * querying 599996791 still returns bogus values (just 3000 state events trailing from SG 397764923).
    * Trying again with no disconnected islands (by failing back to minhash hamming distance if no LSHes match), we get 253K (and some weird jumps) - 1.5% compression.
* aco.py
  * Ant Colony Optimisation solver to TSP which takes the distances matrix output from calc_segmented_tsp.py and generates an ordering from it as a way of doing faster TSP.
  * First cut (100 ants, 100 iterations) converges - but the end result generates 1.7M state rows :/
    * Reduced to 1.0M state rows by considering the DAG as directed TSP (apply pheremones only on fwd path, not return path, which sounds bad for ants, which are symmetric)
      * so 6% compression.
    * The problem seems to be that it jumps very rapidly from segment 0 to a random one (16 distance back and forth), and then stabilises there.
      * Is this because segment 0 is actually an island in terms of LSH bands?
    * Might get fixed by falling back to jaccard on minhashes if jaccard on LSH bands fails?
      * Nope, that seems to make it worse somehow: 1.2M (1231061) rows with directed ACO and minhash fallback (although minhash fallback did help normal TSP)
      * we're still seeing jumps of 128 dist in the ACO, which implies there are either islands or bugs in the TSP solution
    * Alternatively, could we try a heuristic that ants should first try the numerically next unexplored sg_ids rather than random ones if faced with a dead end;
      * ...which only reduces to 1,167,019 rows.
    * Trying that but with undirected graph (just to see if ACO performs better) gives... 1,158,098, so no improvement.
    * There are still loads of misordered state when generating state.
    * Alternatively, do we have a bug in generating the state rows?

* calc_state.py
  * fork of compress_dag_ordered.py which loads the state in the order from calc_branches/hilbert/hamming/segmented_mst/segmented_tsp and compresses it.

Next steps:
 * consider using the state DAG to get better similarity for adjacent temporal table rows
 * try parellised ACO for faster TSP
 * **try falling back to minhash comparison if we fail to find branch points after a jump, to avoid islands which then cause thrashing in any algorithm**
 * try redefining distance 16 to be much higher - won't cause islands, but will discourage hitting it by accident?
 * figure out why 599996791 returns wrong values in the value table

## Dumping state

```sql
\copy (SELECT * FROM matrix.state_groups WHERE room_id = '!OGEhHVWSdvArJzumhm:matrix.org') TO 'sg.csv' WITH CSV HEADER;
\copy (SELECT * FROM matrix.state_group_edges WHERE state_group in (select id from matrix.state_groups where room_id = '!OGEhHVWSdvArJzumhm:matrix.org')) TO 'sge.csv' WITH CSV HEADER;
```
```bash
psql matrix -c "copy (SELECT * FROM matrix.state_groups_state WHERE room_id = '!OGEhHVWSdvArJzumhm:matrix.org') TO stdout WITH CSV HEADER;" | pv | zstd -T0 --long=27 -19 > sgs.zstd
```

## Loading state

```bash
cat sg.csv| psql test -c 'COPY state_groups FROM STDIN WITH CSV HEADER'
cat sge.csv| psql test -c 'COPY state_group_edges FROM STDIN WITH CSV HEADER'
zstdcat sgs.zstd | pv | psql test -c 'COPY state_groups_state FROM STDIN WITH CSV HEADER'
```

## Perf

 Running this on the first 85K SGs of Matrix HQ (out of 410K):
  * Takes 1h of calc_minhash (but isn't remotely memoised or parallelised yet)
  * Takes 10 minutes to find all the branch points (2391 segments)
  * TSP via elkai takes 5 hours, and produces 310K temporal state table rows (not great)
  * TSP via ACO takes about 60 seconds.

## Compare with rust-synapse-state-compressor

...gives 30% compression with default params on HQ:

```
% time ./synapse_compress_state -p "postgresql://localhost/test" -r '!OGEhHVWSdvArJzumhm:matrix.org' -o out.sql -t
Fetching state from DB for room '!OGEhHVWSdvArJzumhm:matrix.org'...
  [2m] 16921727 rows retrieved                                                                                                                                                                                                                                                      Got initial state from database. Checking for any missing state groups...
Fetched state groups up to 599996915
Number of state groups: 83052
Number of rows in current table: 16921672
Compressing state...
[00:15:21] ████████████████████ 83052/83052 state groups                                                                                                                                                                                                                            Number of rows after compression: 5228327 (30.90%)
Compression Statistics:
  Number of forced resets due to lacking prev: 42
  Number of compressed rows caused by the above: 203834
  Number of state groups changed: 7691
Checking that state maps match...
[00:08:06] ████████████████████ 83052/83052 state groups                                                                                                                                                                                                                            New state map matches old one
Writing changes...
[00:00:04] ████████████████████ 83052/83052 state groups                                                                                                                                                                                                                            ./synapse_compress_state -p "postgresql://localhost/test" -r  -o out.sql -t  4941.68s user 17.43s system 324% cpu 25:30.51 total
```