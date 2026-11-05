-- Runs once, when the Postgres volume is first initialised.
-- Superset keeps its own metadata database, separate from the warehouse.
CREATE DATABASE superset;
