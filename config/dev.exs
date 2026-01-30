import Config

# Do not include metadata nor timestamps in development logs
config :logger, :console, format: "[$level] $message\n"

config :mix_test_watch, clear: true

config :eventstore, DevEventStore,
  schema: "event_store",
  column_data_type: "jsonb", 
  partitioned_events: false,
  use_pg_partman: false
