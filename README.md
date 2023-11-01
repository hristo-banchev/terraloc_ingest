# Terraloc.Ingest

A configurable library for ingesting CSV data.

The library was built with ingesting geolocation data in mind, but it is generic
enough to be used for any type of data.

## Installation

The package can be installed by adding this to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:terraloc_ingest, git: "https://github.com/hristo-banchev/terraloc_ingest.git", tag: "v0.1.1"}
  ]
end
```

## Use

Using the library requires:

  * an `Ecto.Repo` to handle the DB connections and data insertion
  * an `Ecto.Schema` to handle the data mapping, conversion and validation
  * a path to a CSV file, the headers of which match the fields from the schema

You you can pass these directly to the library:

```elixir
Terraloc.Ingest.ingest(
  MyApp.Repo,
  MyApp.GeoDataA,
  "path/to/geolocation_data.csv"
)
```

You can also store the Ecto modules to a named configuration and use this instead.
For example, in your `config/config.ex`:

```elixir
  config :terraloc_ingest, :ingestions,
    uk_daily: [
      repo: MyApp.Repo,
      schema: MyApp.GeoDataA,
      chunk_size: 500
    ],
    new_york_hourly: [
      repo: MyApp.CustomIngestionRepo,
      schema: MyApp.GeoDataB
    ]
```

And then pass the ingestion name from the config along with the CSV file to the
library:

```elixir
Terraloc.Ingest.ingest(:uk_daily, "path/to/geolocation_data.csv")
```

Another convenience option is to use a Mix task to ingest the CSV file by setting
your config file like in the example above and using an ingestion name:

```bash
$ mix terraloc.ingest new_york_hourly path/to/geolocation_data.csv
```

## Design philosophy

The configuration of the ingestion process is made with flexibility and
simplicity in mind.

In order to allow for extensive data validation, it made sense to utilize an
`Ecto.Schema` as a well-known way to do validations instead of building a new
configurable validation workflow from scratch in the library.

Utilizing `Ecto.Repo` for storing the data in the database goes along with the
convenience of using an `Ecto.Schema`. It enables the ingestion to work with a
variety of RDBMSs. The user is free to either use the app's general Ecto repo or
define a custom repo, configured specifically to the needs of the ingestion.