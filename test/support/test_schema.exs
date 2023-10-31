defmodule Terraloc.TestSchema do
  use Ecto.Schema
  import Ecto.Changeset

  schema "geolocations" do
    field(:city, :string)
    field(:country, :string)
    field(:country_code, :string)
    field(:ip_address, :string)
    field(:latitude, :decimal)
    field(:longitude, :decimal)
    field(:mystery_value, :string)

    timestamps()
  end

  @doc false
  def changeset(geolocation, attrs) do
    geolocation
    |> cast(attrs, [
      :ip_address,
      :country_code,
      :country,
      :city,
      :latitude,
      :longitude,
      :mystery_value
    ])
    |> validate_required([:ip_address, :country_code, :country, :city, :latitude, :longitude])
    |> validate_length(:ip_address, min: 7, max: 15)
    |> validate_length(:country_code, min: 2, max: 3)
    |> validate_length(:country, max: 100)
    |> validate_length(:city, max: 100)
    |> validate_number(:latitude, greater_than_or_equal_to: -90, less_than_or_equal_to: 90)
    |> validate_number(:longitude, greater_than_or_equal_to: -180, less_than_or_equal_to: 180)
    |> unique_constraint(:ip_address)
  end
end
