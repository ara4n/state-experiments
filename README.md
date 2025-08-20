# Synapse state storage experiments

* compress.py
  * walks synapse's SG tables for a given room_id, turning them into a new temporal table called `state`, which tracks in which SG ID each event_id got added and removed from the current state set.
  * the resulting temporal table is 8436 rows (from 79690 state group state rows, and across 7376 state groups, max room state of 465 state events, for #nvi)
* compress_memoised.py
  * does the same, but shifts most of the processing from SQL to Python and collapses the results of the recursive SGS queries, speeding things up ~100x and saving RAM.  #nvi goes from 30s to 500ms or so, and 6MB of RAM.  Can handle big tables like Matrix HQ (150GB) in an hour or so.
  * However, the algorithm proves not to do well in the face of unstable state, especially when Synapse flipflops storing different SGs in the event of races of lots of state traffic and/or state resets.  This means Matrix HQ only compresses down to 8GB; it should be much better.
* compress_dag_ordered.py
  * tries to improve compression by ordering the SGs not by ID, but topologically by Kahn (and then by ID, given the DAG is split into ~100 SG chunks).
  * The optimisation doesn't seem to buy much; down to 8214 rows for #nvi - Kahn doesn't consider similarity, after all.
  * For one thing, all the ~100 item chunks don't get ordered with respect to each other, other than chronologically (which they already are)
* compress_minhash.py
  * calculates minhashes & LSH bands for each SG, to visualise when SGs are flipflopping
* colorize.py
  * helps visualise LSH bands by colourcoding them on a 24-bit capable terminal
* calc_minhash.py
  * generates minhashes SG by SG, rather than focusing on creating a temporal table, which we can then use to calculate a better ordering based on minhashes
* calc_branches.py
  * goes through the minhashes table, looking for big jumps in current state, and then querying minhashes to find a better ordering by grouping the SGs into 'branches'
  * except this doesn't work very well, as we just end up shifting the big jumps to the *end* of the reordered sequence.
  * in fact, this ends up compressing #nvi to 8382 rows.
* calc_state.py
  * fork of compress_dag_ordered.py which loads the state in the order from calc_branches and compresses it.

Next steps:
 * consider using the state DAG to get better similarity for adjacent temporal table rows
 * consider optimising to minimise jumps at both start & end of segments when reordering
 * consider ordering the whole thing by minhash proximity?
   * order LSH buckets using a space-filling curve of some kind? (z-order, hilbert?)
   * or clustering and space-filling curve on the centers?
   * or simulated annealing or some other fancy optimisation alg?
