import Config

# Configuration globale de EventStore
config :eventstore, EventStore,
  partitioned_events: true  # Default false, set to true if you want a partioned events table

import_config "#{Mix.env()}.exs"
