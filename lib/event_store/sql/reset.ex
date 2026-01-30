defmodule EventStore.Sql.Reset do
  @moduledoc false

  # PostgreSQL statements to reset an event store schema.

  def statements(config) do
    schema = Keyword.fetch!(config, :schema)
    partitioned = Keyword.get(config, :partitioned_events, false)
    partman = Keyword.get(config, :use_pg_partman, false)

    [
      ~s(SET LOCAL search_path TO "#{schema}";),
      ~s(SET LOCAL eventstore.reset TO 'on';)
    ]
    ++ 
      if partitioned and partman do
        undo_partman_partitions(schema)
      else
        []
      end
    ++ truncate_tables(partitioned, schema) ++
    [
      seed_all_stream(schema)
    ]
  end

  defp undo_partman_partitions(_schema) do
    []
  end

  defp _undo_partman_partitions(schema) do
    [
      """
       CREATE OR REPLACE FUNCTION #{schema}.undo_events_partitions_if_exist(
         schema_name TEXT
       ) 
       RETURNS VOID AS $$
       DECLARE
         r RECORD;
         dropped BOOLEAN;
       BEGIN
         dropped := FALSE;
         -- Loop on existing partitions to undo them
         FOR r IN
           SELECT
             child.relname AS partition_name
           FROM pg_inherits
           JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
           JOIN pg_class child  ON pg_inherits.inhrelid = child.oid
           JOIN pg_namespace nsp ON parent.relnamespace = nsp.oid
           WHERE parent.relname = 'events'
             AND nsp.nspname = schema_name
         LOOP
           EXECUTE format('
             DROP TABLE %s.%s CASCADE;
           ', schema_name, r.partition_name);
           dropped := TRUE;
         END LOOP;
         IF dropped THEN
           EXECUTE format('
             DELETE FROM partman.part_config WHERE parent_table = ''%s.events'';
           ', schema_name);
         END IF;
       END;$$
       LANGUAGE plpgsql;
      """,
      "SELECT #{schema}.undo_events_partitions_if_exist('#{schema}');"
    ]
  end

  defp truncate_tables(partitioned, schema) do
    events_root = if partitioned do
      ", #{schema}.events_root"
    else
      "" 
    end
    [
      """
      TRUNCATE TABLE #{schema}.snapshots, #{schema}.subscriptions, #{schema}.stream_events, #{schema}.streams, #{schema}.events#{events_root}
      RESTART IDENTITY;
      """
    ]
  end

  # Create `$all` stream
  defp seed_all_stream(schema) do
    """
    INSERT INTO #{schema}.streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
    """
  end
end
