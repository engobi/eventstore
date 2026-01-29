defmodule EventStore.Sql.Init do
  @moduledoc false

  # PostgreSQL statements to intialize an event store schema.

  def create_partitioned_or_not_events_table(partitioned, schema, column_data_type) do
    if partitioned do
      create_partitioned_events_table(schema, column_data_type)
    else
      create_events_table(schema, column_data_type)
    end
  end

  def statements(config) do
    column_data_type = Keyword.fetch!(config, :column_data_type)
    schema = Keyword.fetch!(config, :schema) || 'event_store'
    database = Keyword.fetch!(config, :database)
    partitioned = Keyword.get(config, :partitioned_events, false)
    partman = Keyword.get(config, :use_pg_partman, false)

    [
      ~s(SET LOCAL search_path TO "#{schema}";),
      create_streams_table(schema),
      create_stream_uuid_index(schema),
      create_events_root_table(partitioned, schema),
      create_partitioned_or_not_events_table(partitioned, schema, column_data_type)
    ] ++ create_events_indexes(schema, column_data_type) ++
    [
      create_stream_events_table(partitioned, schema),
      create_stream_events_index(schema),
      create_event_store_exception_function(schema),
      create_event_store_delete_function(schema),
      prevent_streams_delete(schema),
      prevent_event_delete(schema),
      prevent_event_root_delete(partitioned, schema),
      prevent_event_update(schema),
      prevent_event_root_update(partitioned, schema),
      prevent_stream_events_delete(schema),
      prevent_stream_events_update(schema),
      create_notify_events_function(schema),
      seed_all_stream(schema),
      create_event_notification_trigger(schema),
      create_subscriptions_table(schema),
      create_subscription_index(schema),
      create_snapshots_table(schema, column_data_type),
      create_schema_migrations_table(schema),
      record_event_store_schema_version(schema)
    ] ++ create_events_partitions(partitioned, database, schema, column_data_type, partman)
  end

  defp create_streams_table(schema) do
    """
    CREATE TABLE #{schema}.streams
    (
        stream_id bigserial PRIMARY KEY NOT NULL,
        stream_uuid text NOT NULL,
        stream_version bigint default 0 NOT NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL,
        deleted_at timestamp with time zone
    );
    """
  end

  defp create_stream_uuid_index(schema) do
    """
    CREATE UNIQUE INDEX ix_streams_stream_uuid ON #{schema}.streams (stream_uuid);
    """
  end

  # Create `$all` stream
  defp seed_all_stream(schema) do
    """
    INSERT INTO #{schema}.streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
    """
  end

  # Create `events_root` table
  defp create_events_root_table(partitioned, schema) do
    if partitioned do
      """
      CREATE TABLE IF NOT EXISTS #{schema}.events_root
      (
          event_id uuid PRIMARY KEY NOT NULL
      );
      """
    else
      "SELECT 1;"
    end
  end

  # Create partitioned `events` parent table
  defp create_partitioned_events_table(schema, column_data_type) do
    """
    CREATE TABLE IF NOT EXISTS #{schema}.events (
        event_id UUID NOT NULL,
        event_type TEXT NOT NULL,
        causation_id UUID NULL,
        correlation_id UUID NULL,
        "data" #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
        created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
        CONSTRAINT event_store_events_pkey PRIMARY KEY (event_id, created_at),
        CONSTRAINT event_store_events_root_fk
            FOREIGN KEY (event_id)
            REFERENCES #{schema}.events_root (event_id)
    ) PARTITION BY RANGE (created_at);
    """
  end

  # Create partitioned `events` children tables
  defp create_events_partitions(_partitioned=true, database, schema, _column_data_type, _partman=true) do
    today = Date.utc_today()
    year = to_string(today.year)
    month = String.pad_leading(to_string(today.month), 2, "0")
    [
      "CREATE SCHEMA IF NOT EXISTS partman;",
      "CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;"
    ] ++ create_role(schema, 'partman_user') ++
    [
      "GRANT ALL ON SCHEMA partman TO partman_user;",
      "GRANT ALL ON ALL TABLES IN SCHEMA partman TO partman_user;",
      "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA partman TO partman_user;",
      "GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA partman TO partman_user;",
      "GRANT ALL ON SCHEMA #{schema} TO partman_user;",
      "GRANT TEMPORARY ON DATABASE #{database} to partman_user;",
      """
      CREATE OR REPLACE FUNCTION #{schema}.create_events_partitions(
        schema_name TEXT,
        year TEXT,
        month TEXT
      )
      RETURNS VOID AS $$
      DECLARE
        r RECORD;
        start_date TEXT;
        found BOOLEAN;
      BEGIN
        start_date := year || '-' || month || '-01';
        SELECT EXISTS(
           SELECT
             child.relname AS partition_name
           FROM pg_inherits
           JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
           JOIN pg_class child  ON pg_inherits.inhrelid = child.oid
           JOIN pg_namespace nsp ON parent.relnamespace = nsp.oid
           WHERE parent.relname = 'events'
             AND nsp.nspname = schema_name
        ) INTO found;
        IF NOT found THEN
          IF to_regclass('partman.part_config') IS NOT NULL THEN
            DELETE FROM partman.part_config WHERE parent_table = '#{schema}.events';
          END IF;
          EXECUTE format('
            SELECT partman.create_parent(
             p_parent_table := ''%I.events'',
             p_control      := ''created_at'',
             p_interval     := ''1 month'',
             p_start_partition := ''%I''
           )', schema_name, start_date);
        END IF;
      END;$$
      LANGUAGE plpgsql;
      """,
      "SELECT #{schema}.create_events_partitions('#{schema}', '#{year}', '#{month}');"
    ]
  end

  defp create_events_partitions(_partitioned=true, database, schema, column_data_type, _partman=false) do
    years_months = 0..6 |> Enum.map( fn x ->
      today = Date.utc_today()

      total_months =
        today.year * 12 +
        (today.month - 1) + x

      year = div(total_months, 12)
      month = rem(total_months, 12) + 1

      {to_string(year), String.pad_leading(to_string(month), 2, "0")}
    end)
    create_events_partition(years_months, [], database, schema, column_data_type) ++
    [
      """
      CREATE TABLE #{schema}.events_default PARTITION OF #{schema}.events  DEFAULT;
      """,
      """
      CREATE INDEX IF NOT EXISTS events_default_created_at_idx ON #{schema}.events_default USING btree (created_at);
      """,
      """
      CREATE INDEX IF NOT EXISTS events_default_event_type_created_at_idx ON #{schema}.events_default USING btree (event_type, created_at)
      """
    ]
  end

  defp create_events_partitions(false, _, _, _, _) do
    []
  end

  def create_events_partition( [{_, _}], acc, _, _, _) do
    acc
  end

  def create_events_partition(
    [{start_year, start_month} | list_years_months],
    acc,
    database,
    schema,
    column_data_type) do
    partition_name = start_year <> start_month <> "01"
    {end_year, end_month} = hd(list_years_months)
    partition = [
      """
      CREATE TABLE #{schema}.events_p#{partition_name} PARTITION OF #{schema}.events
        FOR VALUES FROM ('#{start_year}-#{start_month}-01 00:00:00+01') TO ('#{end_year}-#{end_month}-01 00:00:00+01');
      """,
      """
      CREATE INDEX IF NOT EXISTS events_#{partition_name}_created_at_idx ON #{schema}.events_p#{partition_name} USING btree (created_at);
      """
    ] ++
    if String.downcase(column_data_type) == 'jsonb' do
      [
        """
        CREATE INDEX IF NOT EXISTS events_p#{partition_name}_data_idx ON #{schema}.events_p#{partition_name} USING gin (data jsonb_path_ops);
        """
      ]
    else
      ["SELECT 1;"]
    end ++
    [
      """
      CREATE INDEX IF NOT EXISTS events_p#{partition_name}_event_type_created_at_idx
        ON #{schema}.events_p#{partition_name} USING btree (event_type, created_at);
      """
    ]
    create_events_partition(list_years_months, acc ++ partition, database, schema, column_data_type)
  end

  # Create role
  defp create_role(schema, name) do
    [
      """
      CREATE OR REPLACE FUNCTION #{schema}.create_role_if_not_exists(role_name TEXT)
      RETURNS VOID AS $$
      BEGIN
        -- Check if role already exists
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
          EXECUTE format('CREATE ROLE %I WITH LOGIN', role_name);
        END IF;
      END;$$
      LANGUAGE plpgsql;
      """,
      "SELECT #{schema}.create_role_if_not_exists('#{name}');"
    ]
  end

  # Create `events` table
  defp create_events_table(schema, column_data_type) do
    """
    CREATE TABLE #{schema}.events
    (
        event_id uuid PRIMARY KEY NOT NULL,
        event_type text NOT NULL,
        causation_id uuid NULL,
        correlation_id uuid NULL,
        data #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  # Create `events` indexes
  defp create_events_indexes(schema, column_data_type) do
    indexes_queries =
    [
      """
      CREATE INDEX IF NOT EXISTS event_store_events_created_at_idx ON #{schema}.events(created_at);
      """,
      """
      CREATE INDEX IF NOT EXISTS event_store_events_event_type_idx ON #{schema}.events(event_type, created_at);
      """
    ]
    # Adding an index if data is a jsonb
    if String.downcase(column_data_type) == "jsonb" do
      indexes_queries ++
      [
        """
        CREATE INDEX IF NOT EXISTS event_store_events_data_idx ON #{schema}.events USING GIN ("data" jsonb_path_ops);
        """
      ]
    else
      indexes_queries
    end
  end

  defp create_event_store_exception_function(schema) do
    """
    CREATE OR REPLACE FUNCTION #{schema}.event_store_exception()
      RETURNS trigger AS $$
    DECLARE
      message text;
    BEGIN
      message := 'EventStore: ' || TG_ARGV[0];

      RAISE EXCEPTION USING MESSAGE = message;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  # Prevent DELETE operations unless hard deletes have been enabled.
  defp create_event_store_delete_function(schema) do
    """
    CREATE OR REPLACE FUNCTION #{schema}.event_store_delete()
      RETURNS trigger AS $$
    DECLARE
      message text;
    BEGIN
      IF current_setting('eventstore.enable_hard_deletes', true) = 'on' OR
        current_setting('eventstore.reset', true) = 'on'
      THEN
        -- Allow DELETE
        RETURN OLD;
      ELSE
        -- Prevent DELETE
        message := 'EventStore: ' || TG_ARGV[0];

        RAISE EXCEPTION USING MESSAGE = message, ERRCODE = 'feature_not_supported';
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  # prevent updates to `events` table
  defp prevent_event_update(schema) do
    """
      CREATE TRIGGER no_update_events
      BEFORE UPDATE ON #{schema}.events
      FOR EACH STATEMENT
      EXECUTE PROCEDURE #{schema}.event_store_exception('Cannot update events');
    """
  end

  # prevent updates to `events_root` table
  defp prevent_event_root_update(partitioned, schema) do
    if partitioned do
      """
        CREATE TRIGGER no_update_events_root
        BEFORE UPDATE ON #{schema}.events_root
        FOR EACH STATEMENT
        EXECUTE PROCEDURE #{schema}.event_store_exception('Cannot update events_root');
      """
    else
      "SELECT 1;"
    end
  end

  # prevent deletion from `events` table
  defp prevent_event_delete(schema) do
    """
      CREATE TRIGGER no_delete_events
      BEFORE DELETE ON #{schema}.events
      FOR EACH STATEMENT
      EXECUTE PROCEDURE #{schema}.event_store_delete('Cannot delete events');
    """
  end


  # prevent deletion from `events_root` table
  defp prevent_event_root_delete(partitioned, schema) do
    if partitioned do
      """
        CREATE TRIGGER no_delete_events_root
        BEFORE DELETE ON #{schema}.events_root
        FOR EACH STATEMENT
        EXECUTE PROCEDURE #{schema}.event_store_delete('Cannot delete events_root');
      """
    else
      "SELECT 1;"
    end
  end

  defp create_stream_events_table(partitioned, schema) do
    events_table =
      if partitioned do
        "#{schema}.events_root"
      else
        "#{schema}.events"
      end
    """
    CREATE TABLE stream_events
    (
      event_id uuid NOT NULL REFERENCES #{events_table} (event_id),
      stream_id bigint NOT NULL REFERENCES streams (stream_id),
      stream_version bigint NOT NULL,
      original_stream_id bigint REFERENCES streams (stream_id),
      original_stream_version bigint,
      PRIMARY KEY(event_id, stream_id)
    );
    """
  end

  defp create_stream_events_index(schema) do
    """
    CREATE UNIQUE INDEX ix_stream_events ON #{schema}.stream_events (stream_id, stream_version);
    """
  end

  # prevent updates to `stream_events` table
  defp prevent_stream_events_update(schema) do
    """
    CREATE TRIGGER no_update_stream_events
    BEFORE UPDATE ON #{schema}.stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE #{schema}.event_store_exception('Cannot update stream events');
    """
  end

  # prevent deletion from `stream_events` table
  def prevent_stream_events_delete(schema) do
    """
    CREATE TRIGGER no_delete_stream_events
    BEFORE DELETE ON #{schema}.stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE #{schema}.event_store_delete('Cannot delete stream events');
    """
  end

  def prevent_streams_delete(schema) do
    """
    CREATE TRIGGER no_delete_streams
    BEFORE DELETE ON #{schema}.streams
    FOR EACH STATEMENT
    EXECUTE PROCEDURE #{schema}.event_store_delete('Cannot delete streams');
    """
  end

  defp create_notify_events_function(schema) do
    """
    CREATE OR REPLACE FUNCTION #{schema}.notify_events()
      RETURNS trigger AS $$
    DECLARE
      old_stream_version bigint;
      channel text;
      payload text;
    BEGIN
        -- Payload text contains:
        --  * `stream_uuid`
        --  * `stream_id`
        --  * first `stream_version`
        --  * last `stream_version`
        -- Each separated by a comma (e.g. 'stream-12345,1,1,5')

        IF TG_OP = 'UPDATE' THEN
          old_stream_version := OLD.stream_version + 1;
        ELSE
          old_stream_version := 1;
        END IF;

        channel := TG_TABLE_SCHEMA || '.events';
        payload := NEW.stream_uuid || ',' || NEW.stream_id || ',' || old_stream_version || ',' || NEW.stream_version;

        -- Notify events to listeners
        PERFORM pg_notify(channel, payload);

        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  defp create_event_notification_trigger(schema) do
    """
    CREATE TRIGGER event_notification
    AFTER INSERT OR UPDATE ON #{schema}.streams
    FOR EACH ROW EXECUTE PROCEDURE #{schema}.notify_events();
    """
  end

  defp create_subscriptions_table(schema) do
    """
    CREATE TABLE #{schema}.subscriptions
    (
        subscription_id bigserial PRIMARY KEY NOT NULL,
        stream_uuid text NOT NULL,
        subscription_name text NOT NULL,
        last_seen bigint NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  defp create_subscription_index(schema) do
    """
    CREATE UNIQUE INDEX ix_subscriptions_stream_uuid_subscription_name ON #{schema}.subscriptions (stream_uuid, subscription_name);
    """
  end

  defp create_snapshots_table(schema, column_data_type) do
    """
    CREATE TABLE #{schema}.snapshots
    (
        source_uuid text PRIMARY KEY NOT NULL,
        source_version bigint NOT NULL,
        source_type text NOT NULL,
        data #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  # record execution of upgrade scripts
  defp create_schema_migrations_table(schema) do
    """
    CREATE TABLE #{schema}.schema_migrations
    (
        major_version int NOT NULL,
        minor_version int NOT NULL,
        patch_version int NOT NULL,
        migrated_at timestamp with time zone DEFAULT NOW() NOT NULL,
        PRIMARY KEY(major_version, minor_version, patch_version)
    );
    """
  end

  # record current event store schema version
  defp record_event_store_schema_version(schema) do
    """
    INSERT INTO #{schema}.schema_migrations (major_version, minor_version, patch_version)
    VALUES (1, 3, 2);
    """
  end

end
