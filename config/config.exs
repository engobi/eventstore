import Config

# Configuration globale de EventStore
config :eventstore, EventStore,
  partitioned_events: false,  # Default false, set to true if you want a partioned events table
  use_pg_partman: false  # Default false, set to true if you want to use postgresql extension pg_partman

config :eventstore,
  event_stores: [DevEventStore]

import_config "#{Mix.env()}.exs"
