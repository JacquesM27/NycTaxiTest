using System.Text.Json;
using System.Text.Json.Serialization;

var testDate = new DateOnly(2024, 03, 29);
var countryCode = "US";
var year = testDate.Year;

using var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("User-Agent", "NycTaxiTest/1.0");

var url = $"https://date.nager.at/api/v3/PublicHolidays/{year}/{countryCode}";

try
{
    var response = await httpClient.GetStringAsync(url);
    var holidays = JsonSerializer.Deserialize<List<PublicHolidayDto>>(response);

    if (holidays != null)
    {
        var holiday = holidays.SingleOrDefault(h =>
            h.Date.HasValue &&
            DateOnly.FromDateTime(h.Date.Value) == testDate &&
            h.Types != null &&
            h.Types.Contains("Public"));

        if (holiday != null)
        {
            Console.WriteLine($"Data {testDate:yyyy-MM-dd} jest świętem wolnym od pracy:");
            Console.WriteLine($"Nazwa: {holiday.LocalName}");
        }
        else
        {
            Console.WriteLine($"Data {testDate:yyyy-MM-dd} nie jest świętem wolnym od pracy w {countryCode}.");
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Błąd podczas pobierania danych: {ex.Message}");
}

sealed record PublicHolidayDto(
    [property: JsonPropertyName("date")] DateTime? Date,
    [property: JsonPropertyName("localName")] string? LocalName,
    [property: JsonPropertyName("name")] string? Name,
    [property: JsonPropertyName("countryCode")] string? CountryCode,
    [property: JsonPropertyName("fixed")] bool? Fixed,
    [property: JsonPropertyName("global")] bool? Global,
    [property: JsonPropertyName("counties")] List<string>? Counties,
    [property: JsonPropertyName("launchYear")] int? LaunchYear,
    [property: JsonPropertyName("types")] List<string>? Types);