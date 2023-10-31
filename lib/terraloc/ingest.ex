defmodule Terraloc.Ingest do
  @default_chunk_size 1000

  @moduledoc """
  Provides the means to ingest a CSV file by converting and validating each line,
  and inserting the valid data in a database.

  The ingestion requires an Ecto repo and a schema, as well as the path to the
  CSV file. Optionally, you can also specify the number of lines (chunk size)
  processed for a single database insert. It defaults to #{@default_chunk_size}.

      iex> Terraloc.Ingest.ingest(
      ...>   MyApp.Repo,
      ...>   MyApp.GeoDataA,
      ...>   "path/to/20231031_uk_data.csv",
      ...>   500
      ...> )

  You can also store all the configuration except the CSV file path in the config
  files under a custom name and pass that name instead. The chunk size is again
  optional and defaults to #{@default_chunk_size}. For example:

      config :terraloc_ingest, :ingestions,
        uk_daily: [
          repo: MyApp.Repo,
          schema: MyApp.GeoDataA,
          chunk_size: 500
        ],
        new_york_hourly: [
          repo: MyApp.Repo,
          schema: MyApp.GeoDataB
        ]

  And then let the ingestion process read the config:

      iex> Terraloc.Ingest.ingest(:uk_dailly, "path/to/20231031_uk_data.csv")

  """

  @typedoc """
  A map containing metrics about the ingestion process:

    * `:processed` - the total number of processed datapoints (CSV lines)
    * `:imported` - the number of successfully created new records in the database
    * `:invalid` - the number of data points that failed the schema validations
    * `:errors` - the nubmer of data points that resulted in an error during
      database insertion
    * `:elapsed_time` - the total time the whole ingestion process took to finish.
      In microseconds.
  """
  @type metrics :: %{
          processed: non_neg_integer(),
          imported: non_neg_integer(),
          invalid: non_neg_integer(),
          errors: non_neg_integer(),
          elapsed_time: non_neg_integer()
        }

  @doc """
  Reads the content of a CSV file and imports it to a database, processing the
  CSV lines in chunks. Uses the given ingestion name to get the related config
  containing the Ecto repo and schema, and, optionally, chunk size (defaults to
  #{@default_chunk_size}).

  Parameters:

    * `ingestion_name` - The name (atom) under which the config with the Ecto
      repo, schema and chunk size are stored.
    * `csv_path` - the path to the CSV file to be ingested. The CSV headers
      should be named as the schema's fields so that the mapping can work.

  Returns a map with metrics about the ingestion process. If the CSV headers
  don't all match the schema fields, then `{:error, :invalid_csv_headers}` is
  returned.
  """
  @spec ingest(atom(), String.t()) :: {:ok, metrics()} | {:error, :invalid_csv_headers}
  def ingest(ingestion_name, csv_path) when is_atom(ingestion_name) and is_binary(csv_path) do
    Application.get_env(:terraloc_ingest, :ingestions)
    |> Keyword.get(ingestion_name)
    |> case do
      [repo: repo, schema: schema, chunk_size: chunk_size] ->
        ingest(repo, schema, csv_path, chunk_size)

      [repo: repo, schema: schema] ->
        ingest(repo, schema, csv_path)
    end
  end

  @doc """
  Reads the content of a CSV file and imports it to a database using the given
  Ecto repo and schema, processing the CSV lines in chunks.

  Parameters:

    * `repo` - a database repository module that uses `Ecto.Repo`. This repo is
      going to be used for storing the new data entries.
    * `schema` - an `Ecto.Schema` schema module used to convert the CSV data
      accordingly and validate it.
    * `csv_path` - the path to the CSV file to be ingested. The CSV headers
      should be named as the schema's fields so that the mapping can work.
    * `chunk_size` the number of CSV lines that will be processed for a single
      DB insert query. Defaults to #{@default_chunk_size}.

  Returns a map with metrics about the ingestion process. If the CSV headers
  don't all match the schema fields, then `{:error, :invalid_csv_headers}` is
  returned.
  """
  @spec ingest(atom(), atom(), String.t(), non_neg_integer()) :: {:ok, metrics()} | {:error, :invalid_csv_headers}
  def ingest(repo, schema, csv_path, chunk_size \\ @default_chunk_size)
      when is_atom(repo) and is_atom(schema) and is_binary(csv_path) and is_integer(chunk_size) do
    {microseconds, result} =
      :timer.tc(fn ->
        process_csv_file(repo, schema, csv_path, chunk_size)
      end)

    case result do
      metrics when is_map(metrics) ->
        {:ok, Map.put(metrics, :elapsed_time, microseconds)}

      {:error, :invalid_csv_headers} = error ->
        error
    end
  end

  ###########
  # Private #
  ###########

  defp process_csv_file(repo, schema, csv_path, chunk_size) do
    file_stream = File.stream!(csv_path, [read_ahead: 100_000], 1000)

    if valid_csv_headers?(file_stream, schema) do
      ingest_csv_file(repo, schema, file_stream, chunk_size)
    else
      {:error, :invalid_csv_headers}
    end
  end

  defp valid_csv_headers?(file_stream, schema) do
    # Checks whether all CSV headers are actually schema fields.
    # We don't yet trust the CSV headers so we don't convert them to atoms.
    # Instead, the schema field names are converted to strings.
    csv_headers = file_stream |> CSV.decode!() |> Enum.take(1) |> List.first()
    schema_fields = schema.__schema__(:fields) |> Enum.map(&Kernel.to_string/1)

    csv_headers -- schema_fields == []
  end

  defp ingest_csv_file(repo, schema, file_stream, chunk_size) do
    initial_metrics = %{
      processed: 0,
      imported: 0,
      invalid: 0,
      errors: 0
    }

    file_stream
    |> CSV.decode!(headers: true)
    |> Stream.chunk_every(chunk_size)
    |> Enum.reduce(initial_metrics, fn chunk, metrics_acc ->
      chunk_metrics = process_chunk(repo, schema, chunk)

      %{
        processed: metrics_acc.processed + chunk_metrics.processed,
        imported: metrics_acc.imported + chunk_metrics.imported,
        invalid: metrics_acc.invalid + chunk_metrics.invalid,
        errors: metrics_acc.errors + chunk_metrics.errors
      }
    end)
  end

  defp process_chunk(repo, schema, chunk) do
    blank_schema = struct(schema)
    now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    timestamps =
      schema
      |> extract_timestamp_fields()
      |> Enum.reduce(%{}, fn key, acc -> Map.put(acc, key, now) end)

    attrs_list =
      chunk
      |> Enum.map(fn attrs -> schema.changeset(blank_schema, attrs) end)
      |> Enum.filter(fn changeset -> changeset.valid? end)
      |> Enum.map(fn changeset -> Map.merge(changeset.changes, timestamps) end)

    valid_count = length(attrs_list)

    {imported_count, _} = repo.insert_all(schema, attrs_list, on_conflict: :nothing)

    current_chunk_size = length(chunk)

    %{
      processed: current_chunk_size,
      imported: imported_count,
      invalid: current_chunk_size - valid_count,
      errors: valid_count - imported_count
    }
  end

  defp extract_timestamp_fields(schema) do
    schema.__schema__(:autogenerate_fields) && [:inserted_at, :updated_at]
  end
end
