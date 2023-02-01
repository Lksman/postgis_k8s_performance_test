import secrets

db_single_node_details = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password ': secrets.db_single_node_secrets['password'],
    'host': secrets.db_single_node_secrets['host'],
    'port': secrets.db_single_node_secrets['port']
}

db_cluster_details = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': secrets.db_cluster_secrets['password'],
    'host': secrets.db_cluster_secrets['host'],
    'port': secrets.db_cluster_secrets['port']
}

db_local_details = {
    'dbname': 'nyc_streets',
    'user' : 'postgres',
    'password' : secrets.db_local_secrets['password'],
    'host' : secrets.db_local_secrets['host'],
    'port' : secrets.db_local_secrets['port']
}

# possible additional queries: create table
queries = {
    'Union' : 'SELECT ST_AsText(ST_Union(geom)) FROM nyc_census_blocks',

    'ConvexHull' : "SELECT ST_ConvexHull(ST_Union(st.geom)) \
                    FROM nyc_streets AS st \
                    JOIN nyc_neighborhoods AS nb \
                    ON ST_Contains(nb.geom, st.geom) \
                    GROUP BY nb.boroname \
                    HAVING nb.boroname = 'Brooklyn'",
    
    'Intersection' : 'SELECT ST_Intersection(mc.geom, mcb.geom) FROM \
	                    (SELECT ST_AsText(ST_Union(geom)) AS geom FROM nyc_census_blocks) AS mc, \
	                    (SELECT ST_AsText(ST_Buffer(ST_Union(geom), 1000)) AS geom FROM nyc_census_blocks) AS mcb;',

    'DWithin' : 'SELECT sum(distinct popn_total) AS population_within_500m_of_subway \
                        FROM nyc_census_blocks AS cb \
                        JOIN nyc_subway_stations AS sub \
                        ON ST_DWithin(cb.geom, sub.geom, 500)',
}