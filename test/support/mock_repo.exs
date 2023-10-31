defmodule Terraloc.MockStore do
  use Agent

  def start_link(_) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def all do
    Agent.get(__MODULE__, &Map.values/1)
  end

  def insert(key, data) do
    Agent.get_and_update(__MODULE__, fn state ->
      if Map.has_key?(state, key) do
        {{:error, :duplicate}, state}
      else
        {:ok, Map.put(state, key, data)}
      end
    end)
  end
end

defmodule Terraloc.MockRepo do
  def insert_all(_schema, attrs_list, on_conflict: :nothing) do
    total_count =
      Enum.reduce(attrs_list, 0, fn attrs, count ->
        Terraloc.MockStore.insert(attrs.ip_address, attrs)
        |> case do
          :ok ->
            count + 1

          {:error, :duplicate} ->
            count
        end
      end)

    {total_count, nil}
  end

  def all() do
    Terraloc.MockStore.all()
  end
end
