defmodule Terraloc.IngestTest do
  use ExUnit.Case

  require Terraloc.MockRepo, as: MockRepo
  require Terraloc.TestSchema, as: TestSchema

  setup do
    start_link_supervised!(Terraloc.MockStore)

    Application.put_env(:terraloc_ingest, :ingestions,
      ingest_a: [
        repo: MockRepo,
        schema: TestSchema
      ],
      ingest_b: [
        repo: MockRepo,
        schema: TestSchema,
        chunk_size: 2
      ]
    )

    :ok
  end

  describe ".ingest/2" do
    test "should fetch the ingestion config and process a CSV file" do
      assert {:ok, %{elapsed_time: _, errors: 2, imported: 8, invalid: 4, processed: 14}} =
               Terraloc.Ingest.ingest(:ingest_a, "test/fixtures/geoloc_data.csv")
    end

    test "should fetch the ingestion config with a custom chunk size and process a CSV file" do
      assert {:ok, %{elapsed_time: _, errors: 2, imported: 8, invalid: 4, processed: 14}} =
               Terraloc.Ingest.ingest(:ingest_b, "test/fixtures/geoloc_data.csv")
    end

    test "should raise an error for each missing or invalid parameter" do
      assert_raise FunctionClauseError, fn ->
        Terraloc.Ingest.ingest(0, "test/fixtures/geoloc_data.csv")
      end

      assert_raise CaseClauseError, fn ->
        Terraloc.Ingest.ingest(:missing, "test/fixtures/geoloc_data.csv")
      end

      assert_raise FunctionClauseError, fn ->
        Terraloc.Ingest.ingest(:ingest_a, nil)
      end

      assert_raise File.Error, "could not stream \"missing.csv\": no such file or directory", fn ->
        Terraloc.Ingest.ingest(:ingest_a, "missing.csv")
      end
    end

    test "should return an error if the CSV headers don't match the schema fields" do
      assert {:error, :invalid_csv_headers} = Terraloc.Ingest.ingest(:ingest_a, "test/fixtures/wrong_mapping.csv")
    end
  end

  describe ".ingest/4" do
    test "should process a CSV file utilizing the given repo and schema, and the default chunk size" do
      assert {:ok, %{elapsed_time: _, errors: 2, imported: 8, invalid: 4, processed: 14}} =
               Terraloc.Ingest.ingest(MockRepo, TestSchema, "test/fixtures/geoloc_data.csv")
    end

    test "should process a CSV file utilizing the given repo and schema, and a custom chunk size" do
      assert {:ok, %{elapsed_time: _, errors: 2, imported: 8, invalid: 4, processed: 14}} =
               Terraloc.Ingest.ingest(MockRepo, TestSchema, "test/fixtures/geoloc_data.csv", 2)
    end

    test "should raise an error for each missing or invalid parameter" do
      assert_raise FunctionClauseError, fn ->
        Terraloc.Ingest.ingest(0, TestSchema, "test/fixtures/geoloc_data.csv")
      end

      assert_raise UndefinedFunctionError,
                   "function DummyRepo.insert_all/3 is undefined (module DummyRepo is not available)",
                   fn ->
                     Terraloc.Ingest.ingest(DummyRepo, TestSchema, "test/fixtures/geoloc_data.csv")
                   end

      assert_raise FunctionClauseError, fn ->
        Terraloc.Ingest.ingest(MockRepo, 0, "test/fixtures/geoloc_data.csv")
      end

      assert_raise UndefinedFunctionError, fn ->
        Terraloc.Ingest.ingest(MockRepo, DummySchema, "test/fixtures/geoloc_data.csv")
      end

      assert_raise FunctionClauseError, fn ->
        Terraloc.Ingest.ingest(MockRepo, TestSchema, nil)
      end

      assert_raise File.Error, "could not stream \"missing.csv\": no such file or directory", fn ->
        Terraloc.Ingest.ingest(MockRepo, TestSchema, "missing.csv")
      end
    end

    test "should return an error if the CSV headers don't match the schema fields" do
      assert {:error, :invalid_csv_headers} =
               Terraloc.Ingest.ingest(MockRepo, TestSchema, "test/fixtures/wrong_mapping.csv")
    end
  end
end
