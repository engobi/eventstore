import Config

# Global configuration of EventStore
config :eventstore, EventStore,
  partitioned_events: false,  # Set to true if you want a partioned events table
  use_pg_partman: false  # Set to true if you want to use postgresql extension pg_partman

config :eventstore,
  event_stores: [DevEventStore]

import_config "#{Mix.env()}.exs"
