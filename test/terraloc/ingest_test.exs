defmodule Terraloc.IngestTest do
  use ExUnit.Case
  doctest Terraloc.Ingest

  test "greets the world" do
    assert Terraloc.Ingest.hello() == :world
  end
end
