defmodule Mix.Tasks.Terraloc.IngestTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  require Terraloc.MockRepo, as: MockRepo
  require Terraloc.TestSchema, as: TestSchema

  alias Mix.Tasks.Terraloc.Ingest

  setup do
    start_link_supervised!(Terraloc.MockStore)

    Application.put_env(:terraloc_ingest, :ingestions,
      ingest_a: [
        repo: MockRepo,
        schema: TestSchema
      ]
    )

    :ok
  end

  describe "terralock_ingest mix task" do
    test "should successfully ingest a CSV file" do
      log =
        capture_log(fn ->
          assert {:ok, %{elapsed_time: _, errors: 2, imported: 8, invalid: 4, processed: 14}} =
                   Ingest.run(["ingest_a", "test/fixtures/geoloc_data.csv"])
        end)

      assert log =~ "errors: 2, imported: 8, invalid: 4, processed: 14"
    end

    test "should raise an error for each missing or invalid parameter" do
      assert_raise CaseClauseError, fn ->
        Ingest.run(["0", "test/fixtures/geoloc_data.csv"])
      end

      assert_raise ArgumentError, fn ->
        Ingest.run([:missing, "test/fixtures/geoloc_data.csv"])
      end

      assert_raise FunctionClauseError, fn ->
        Ingest.run(["ingest_a", nil])
      end

      assert_raise File.Error, "could not stream \"missing.csv\": no such file or directory", fn ->
        Ingest.run(["ingest_a", "missing.csv"])
      end
    end

    test "should return an error if the CSV headers don't match the schema fields" do
      assert {:error, :invalid_csv_headers} = Terraloc.Ingest.ingest(:ingest_a, "test/fixtures/wrong_mapping.csv")
    end
  end
end
