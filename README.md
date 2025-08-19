# Synapse state storage experiments

* compress.py
  * walks synapse's SG tables for a given room_id, turning them into a new temporal table called `state`, which tracks in which SG ID each event_id got added and removed from the current state set.
* compress_memoised.py
  * does the same, but shifts most of the processing from SQL to Python and collapses the results of the recursive SGS queries, speeding things up ~100x and saving RAM.  Can handle big tables like Matrix HQ (150GB) in an hour or so.
  * However, the algorithm proves not to do well in the face of unstable state, especially when Synapse flipflops storing different SGs in the event of races of lots of state traffic and/or state resets.  This means Matrix HQ only compresses down to 8B; it should be much better.
* compress_dag_ordered.py
  * tries to improve compression by ordering the SGs not by ID, but topologically by Kahn (and then by ID).  The optimisation doesn't buy much though..
* compress_minhash.py
  * calculates minhashes & LSH bands for each SG, to visualise when SGs are flipflopping
* colorize.py
  * helps visualise LSH bands by colourcoding them on a 24-bit capable terminal
* calc_minhash.py
  * generates minhashes SG by SG, rather than focusing on creating a temporal table, which we can then use to calculate a better ordering based on minhashes
* calc_branches.py
  * goes through the minhashes table, looking for big jumps in current state, and then querying minhashes to find a better ordering by grouping the SGs into 'eras' (or 'branches').
* calc_state.py
  * fork of compress_dag_ordered.py which loads the state in the order from calc_branches and compresses it.