defmodule Mix.Tasks.Terraloc.Ingest do
  @moduledoc """
  Ingests a CSV file containing geolocation data.

  The task accepts an ingestion name and a path to a CSV file. The ingestion
  name is used to fetch the related configuration including the Ecto repo and
  schema that will be used during the ingestion process.

  ## Examples

  Place a suitable configuration of the ingestion in your application's
  `config/config.ex` file:

      config :terraloc_ingest, :ingestions,
        new_york_hourly: [
          repo: MyApp.Repo,
          schema: MyApp.GeoDataA
        ]

  Then you can run the Mix task using this configuration like so:

      $ mix terraloc.ingest new_york_hourly path/to/data.csv

  """

  @shortdoc "Ingests a CSV file containing geolocation data"

  @requirements ["app.config", "app.start"]

  use Mix.Task

  require Logger

  @impl Mix.Task
  def run([ingestion_name, csv_path]) do
    result = Terraloc.Ingest.ingest(String.to_atom(ingestion_name), csv_path)

    case result do
      {:ok, metrics} ->
        Logger.info(metrics)

      {:error, _} = error ->
        Logger.info(error)
    end

    result
  end
end
