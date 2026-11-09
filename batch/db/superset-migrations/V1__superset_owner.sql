-- Runs against the superset database. Hands the tables Superset created before it had
-- its own login over to that login, so later upgrades (superset db upgrade) can alter them.
-- On a fresh install there's nothing to move: Superset creates its tables as its own login.

DO $$
DECLARE
    rel record;
BEGIN
    FOR rel IN
        SELECT n.nspname, c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
          AND c.relname <> 'flyway_schema_history'
          AND pg_get_userbyid(c.relowner) <> '${superset_user}'
          AND NOT (c.relkind = 'S' AND EXISTS (
              SELECT FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('a', 'i')))
    LOOP
        EXECUTE format('ALTER %s %I.%I OWNER TO %I',
            CASE rel.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
                             WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END,
            rel.nspname, rel.relname, '${superset_user}');
    END LOOP;
END
$$;

-- Enum types Superset's migrations create belong to it too.
DO $$
DECLARE
    t record;
BEGIN
    FOR t IN
        SELECT typname FROM pg_type
        WHERE typnamespace = 'public'::regnamespace AND typtype = 'e'
          AND pg_get_userbyid(typowner) <> '${superset_user}'
    LOOP
        EXECUTE format('ALTER TYPE public.%I OWNER TO %I', t.typname, '${superset_user}');
    END LOOP;
END
$$;
