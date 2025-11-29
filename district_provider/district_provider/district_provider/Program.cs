using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

List<DistrictDataDto> DistrictData =
[
    new DistrictDataDto(40.7501106, -73.9938965),
    new DistrictDataDto(40.7242432, -74.0016479),
    new DistrictDataDto(40.8027878, -73.9633408),
    new DistrictDataDto(40.7138176, -74.0090866),
    new DistrictDataDto(40.7624283, -73.9711761),
    new DistrictDataDto(40.7740479, -73.8743744),
    new DistrictDataDto(40.7260094, -73.9832764),
    new DistrictDataDto(40.7341423, -74.0026627),
    new DistrictDataDto(40.6443558, -73.7830429),
    new DistrictDataDto(40.7679482, -73.9855881),
    new DistrictDataDto(40.7231026, -73.9886169),
    new DistrictDataDto(40.7514191, -73.9937820),
    new DistrictDataDto(40.7043762, -74.0083618)
];

using var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("User-Agent", "NycTaxiTest/1.0");

foreach (var location in DistrictData)
{
    var url = $"https://nominatim.openstreetmap.org/reverse?lat={location.Longitude.ToString(CultureInfo.InvariantCulture)}&lon={location.Latitude.ToString(CultureInfo.InvariantCulture)}&format=json";

    try
    {
        var response = await httpClient.GetStringAsync(url);
        var nominatimResponse = JsonSerializer.Deserialize<NominatimResponse>(response);

        if (nominatimResponse != null)
        {
            Console.WriteLine($"Location ({location.Longitude}, {location.Latitude}):");
            Console.WriteLine($"Place: {nominatimResponse.Name}");
            Console.WriteLine($"Borough: {nominatimResponse.Address?.Suburb}");
            Console.WriteLine($"Neighborhood: {nominatimResponse.Address?.Neighbourhood}");
        }

        // Nominatim requires 1 second between requests
        await Task.Delay(1000);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error fetching data for ({location.Longitude}, {location.Latitude}): {ex.Message}");
    }
}

sealed record DistrictDataDto(
    double Longitude,
    double Latitude);

sealed record NominatimResponse(
    [property: JsonPropertyName("place_id")] long PlaceId,
    [property: JsonPropertyName("licence")] string? Licence,
    [property: JsonPropertyName("osm_type")] string? OsmType,
    [property: JsonPropertyName("osm_id")] long OsmId,
    [property: JsonPropertyName("lat")] string? Lat,
    [property: JsonPropertyName("lon")] string? Lon,
    [property: JsonPropertyName("class")] string? Class,
    [property: JsonPropertyName("type")] string? Type,
    [property: JsonPropertyName("place_rank")] int PlaceRank,
    [property: JsonPropertyName("importance")] double Importance,
    [property: JsonPropertyName("addresstype")] string? AddressType,
    [property: JsonPropertyName("name")] string? Name,
    [property: JsonPropertyName("display_name")] string? DisplayName,
    [property: JsonPropertyName("address")] NominatimAddress? Address,
    [property: JsonPropertyName("boundingbox")] List<string>? BoundingBox);

sealed record NominatimAddress(
    [property: JsonPropertyName("leisure")] string? Leisure,
    [property: JsonPropertyName("house_number")] string? HouseNumber,
    [property: JsonPropertyName("road")] string? Road,
    [property: JsonPropertyName("neighbourhood")] string? Neighbourhood,
    [property: JsonPropertyName("suburb")] string? Suburb,
    [property: JsonPropertyName("county")] string? County,
    [property: JsonPropertyName("city")] string? City,
    [property: JsonPropertyName("state")] string? State,
    [property: JsonPropertyName("ISO3166-2-lvl4")] string? ISO31662Lvl4,
    [property: JsonPropertyName("postcode")] string? Postcode,
    [property: JsonPropertyName("country")] string? Country,
    [property: JsonPropertyName("country_code")] string? CountryCode);