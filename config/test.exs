import Config

config :logger, backends: []

config :ex_unit,
  capture_log: [level: :warning],
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 100

default_config = [
  idle_interval: 100,
  username: "postgres",
  password: "postgres",
  database: "eventstore_test",
  hostname: "localhost",
  schema: "public",
  pool_size: 1,
  serializer: EventStore.JsonSerializer,
  subscription_retry_interval: 1_000,
  partitioned_events: false,  # Default false, set to true if you want a partioned events table
  use_pg_partman: false,
  column_data_type: "jsonb"
]

config :eventstore, TestEventStore, default_config
config :eventstore, SecondEventStore, Keyword.put(default_config, :database, "eventstore_test_2")
config :eventstore, SchemaEventStore, default_config

config :eventstore, event_stores: [TestEventStore, SecondEventStore, SchemaEventStore]
