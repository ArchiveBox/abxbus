import Config

config :abx_bus,
  default_event_concurrency: :bus_serial,
  default_handler_concurrency: :parallel,
  default_handler_completion: :all,
  default_max_history_size: 1000
