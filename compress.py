#!/usr/bin/env python3

import psycopg2

# go through the state groups in order of SG ID
# create a new table called state:
# start_sg_id, end_sg_id, event_id, room_id, type, state_key
# calculate next state at the next sg_id (by recursing through s_g_e as in today's synapse)
# diff sets of before & after
# insert new state events which have now materialised at this point
# set end_sg_id for the current state for ones which disappear at the next row.

# Disadvantage is that we can't cache state groups, but perhaps it'll be so fast we don't need to.
# ...or we can create the state group optimisation in the cache rather than the DB on demand
# by factoring out commonality in the results coughed up by the DB.
# Other problem is that state resets which thrash the state of the room will cause the table to grow, but
# it will still be way better than the previous incarnation. Plus we may be able to detect and throw away
# thrashing as part of mitigating state spam.

DB_CONFIG = {
    # 'host': 'localhost',
    'database': 'test',
    # 'user': 'your_username',
    # 'password': 'your_password',
    # 'port': '5432'
}

# CREATE TABLE state (
#   start_sg_id bigint not null,
#   end_sg_id bigint,
#   event_id text,
#   room_id text,
#   type text,
#   state_key text
# );

try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(autocommit=True)
    cursor = conn.cursor()

    def get_sg(sg_id):
        # taken from synapse's _get_state_groups_from_groups_txn
        sql = """
        WITH RECURSIVE sgs(state_group) AS (
            VALUES(%s::bigint)
            UNION ALL
            SELECT prev_state_group FROM state_group_edges e, sgs s
            WHERE s.state_group = e.state_group
        )
        SELECT DISTINCT ON (type, state_key)
            event_id
            FROM state_groups_state
            WHERE state_group IN (
                SELECT state_group FROM sgs
            )
            ORDER BY type, state_key, state_group DESC
        """
        c = conn.cursor()
        c.execute(sql, [sg_id])
        res = { row[0] for row in c.fetchall() }
        c.close()
        return res
    
    def add_state(sg_id, event_id):
        sql = """
        INSERT INTO state (start_sg_id, event_id, room_id, type, state_key)
        SELECT %s, %s, room_id, type, state_key FROM state_groups_state
        WHERE event_id = %s
        LIMIT 1
        """
        print ("add", sg_id, event_id)
        c = conn.cursor()
        c.execute(sql, [sg_id, event_id, event_id])
        if c.rowcount == 0:
            print("Failed to insert into state")

        c.close()

    def mark_state_as_gone(last_sg_id, event_id):
        # events can appear multiple times in the table due to appearing and disappearing on different paths
        # so grab the most recent one, when it disappears.
        # N.B. this means that if you have two significantly competing forks flickering back and forth, we could
        # end up with a lot of noise in the table.
        print ("kill", last_sg_id, event_id)
        sql = """
        UPDATE state SET end_sg_id=%s
        WHERE event_id = %s and start_sg_id = (
            SELECT start_sg_id
            FROM state
            WHERE event_id = %s
            ORDER BY start_sg_id DESC LIMIT 1 
        )
        """
        c = conn.cursor()
        c.execute(sql, [last_sg_id, event_id, event_id ])
        if c.rowcount == 0:
            print("Failed to update state")
        c.close()

    room_id = '!kxwQeJPhRigXSZrHqf:matrix.org'
    cursor.execute("SELECT id FROM state_groups where room_id=%s order by id", [room_id])
    state_groups = [ row[0] for row in cursor.fetchall() ]
    cursor.close()

    state_set = set()
  
    # todo: can we do this all in SQL?
    for sg_id in state_groups:
        print (sg_id)
        new_state_set = get_sg(sg_id)
        new_ids = new_state_set - state_set
        gone_ids = state_set - new_state_set
        # print("new_ids", new_ids)
        # print("gone_ids", gone_ids)
        for event_id in new_ids:
            add_state(sg_id, event_id)
        for event_id in gone_ids:
            mark_state_as_gone(sg_id, event_id)
        state_set = new_state_set

    # then, we can query current state with:
    # select * from state where end_sg_id is null;
    #
    # or for historic state, with:
    # select * from state where start_sg_id <= 747138778 and (end_sg_id is null or end_sg_id > 747138778);

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# Our existing synapse schema, for reference

# CREATE TABLE state_groups_state (
#     state_group bigint,
#     room_id text,
#     type text,
#     state_key text,
#     event_id text
# );
# CREATE INDEX state_groups_state_room_id_idx ON state_groups_state USING brin (room_id) WITH (pages_per_range='1');
# CREATE INDEX state_groups_state_type_idx_new ON state_groups_state USING btree (state_group, type, state_key);

# CREATE TABLE state_groups (
#     id bigint NOT NULL,
#     room_id text NOT NULL,
#     event_id text NOT NULL
# );
# ALTER TABLE ONLY state_groups
#     ADD CONSTRAINT state_groups_pkey PRIMARY KEY (id);
# CREATE INDEX state_groups_room_id_idx ON state_groups USING btree (room_id);

# CREATE TABLE state_group_edges (
#     state_group bigint NOT NULL,
#     prev_state_group bigint NOT NULL
# );
# CREATE INDEX state_group_edges_prev_idx ON state_group_edges USING btree (prev_state_group);
# CREATE UNIQUE INDEX state_group_edges_unique_idx ON state_group_edges USING btree (state_group, prev_state_group);

