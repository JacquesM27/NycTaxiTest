using System.Data;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

const string sqlServerConnectionString = "Data Source=JAKUB\\IDH2022;Database=NycTaxi;Trusted_Connection=True;MultipleActiveResultSets=true;TrustServerCertificate=True;";
var filePath = "C:\\pjatk\\studia magisterskie\\sem3\\idh\\nyc tazi\\yellow_tripdata_2015-01.csv";
var filePath1 = "C:\\pjatk\\studia magisterskie\\sem3\\idh\\nyc tazi\\yellow_tripdata_2016-01.csv";
var filePath2 = "C:\\pjatk\\studia magisterskie\\sem3\\idh\\nyc tazi\\yellow_tripdata_2016-02.csv";
var filePath3 = "C:\\pjatk\\studia magisterskie\\sem3\\idh\\nyc tazi\\yellow_tripdata_2016-03.csv";
const int batchSize = 50000;

var filesToProcess = new[] { filePath, filePath1, filePath2, filePath3 };

Console.WriteLine("=== NYC Taxi Data Import - Ultra Fast Mode ===");
Console.WriteLine($"Liczba plików: {filesToProcess.Length}");
Console.WriteLine($"Batch size: {batchSize:N0}");
Console.WriteLine($"Start: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
Console.WriteLine();

var totalStopwatch = Stopwatch.StartNew();
long totalRecords = 0;
int batchNumber = 0;

try
{
    using var connection = new SqlConnection(sqlServerConnectionString);
    await connection.OpenAsync();

    var dataTable = CreateDataTable();
    var batchStopwatch = new Stopwatch();

    for (var fileIndex = 0; fileIndex < filesToProcess.Length; fileIndex++)
    {
        var currentFilePath = filesToProcess[fileIndex];
        var fileName = Path.GetFileName(currentFilePath);

        Console.WriteLine($">>> Przetwarzanie pliku {fileIndex + 1}/{filesToProcess.Length}: {fileName}");

        using var reader = new StreamReader(currentFilePath);

        await reader.ReadLineAsync();

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line)) continue;

            var record = ParseCsvLine(line, fileName);
            if (record != null)
            {
                AddRowToDataTable(dataTable, record);
                totalRecords++;

                if (dataTable.Rows.Count >= batchSize)
                {
                    batchStopwatch.Restart();
                    await BulkInsertBatch(connection, dataTable);
                    batchStopwatch.Stop();

                    batchNumber++;
                    var recordsPerSecond = (double)batchSize / batchStopwatch.Elapsed.TotalSeconds;

                    Console.WriteLine($"Batch #{batchNumber:N0} | Rekordów: {batchSize:N0} | " +
                                    $"Czas: {batchStopwatch.ElapsedMilliseconds:N0} ms | " +
                                    $"Prędkość: {recordsPerSecond:N0} rek/s | " +
                                    $"Total: {totalRecords:N0}");

                    dataTable.Clear();
                }
            }
        }

        Console.WriteLine($"<<< Zakończono plik: {fileName}");
        Console.WriteLine();
    }

    if (dataTable.Rows.Count > 0)
    {
        batchStopwatch.Restart();
        await BulkInsertBatch(connection, dataTable);
        batchStopwatch.Stop();

        batchNumber++;
        var recordsPerSecond = (double)dataTable.Rows.Count / batchStopwatch.Elapsed.TotalSeconds;

        Console.WriteLine($"Batch #{batchNumber:N0} (ostatni) | Rekordów: {dataTable.Rows.Count:N0} | " +
                        $"Czas: {batchStopwatch.ElapsedMilliseconds:N0} ms | " +
                        $"Prędkość: {recordsPerSecond:N0} rek/s | " +
                        $"Total: {totalRecords:N0}");
    }

    totalStopwatch.Stop();

    Console.WriteLine();
    Console.WriteLine("=== PODSUMOWANIE ===");
    Console.WriteLine($"Łącznie rekordów: {totalRecords:N0}");
    Console.WriteLine($"Liczba partii: {batchNumber:N0}");
    Console.WriteLine($"Całkowity czas: {totalStopwatch.Elapsed:hh\\:mm\\:ss\\.fff}");
    Console.WriteLine($"Średnia prędkość: {totalRecords / totalStopwatch.Elapsed.TotalSeconds:N0} rek/s");
    Console.WriteLine($"Koniec: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
}
catch (Exception ex)
{
    Console.WriteLine($"BŁĄD: {ex.Message}");
    Console.WriteLine(ex.StackTrace);
}

static DataTable CreateDataTable()
{
    var dt = new DataTable();

    dt.Columns.Add("VendorID", typeof(int));
    dt.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
    dt.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
    dt.Columns.Add("passenger_count", typeof(int));
    dt.Columns.Add("trip_distance", typeof(decimal));
    dt.Columns.Add("pickup_longitude", typeof(decimal));
    dt.Columns.Add("pickup_latitude", typeof(decimal));
    dt.Columns.Add("RateCodeID", typeof(int));
    dt.Columns.Add("store_and_fwd_flag", typeof(string));
    dt.Columns.Add("dropoff_longitude", typeof(decimal));
    dt.Columns.Add("dropoff_latitude", typeof(decimal));
    dt.Columns.Add("payment_type", typeof(int));
    dt.Columns.Add("fare_amount", typeof(decimal));
    dt.Columns.Add("extra", typeof(decimal));
    dt.Columns.Add("mta_tax", typeof(decimal));
    dt.Columns.Add("tip_amount", typeof(decimal));
    dt.Columns.Add("tolls_amount", typeof(decimal));
    dt.Columns.Add("improvement_surcharge", typeof(decimal));
    dt.Columns.Add("total_amount", typeof(decimal));
    dt.Columns.Add("SourceFileName", typeof(string));

    return dt;
}

static NYCTaxiRecord? ParseCsvLine(string line, string fileName)
{
    try
    {
        var parts = line.Split(',');
        if (parts.Length < 19) return null;

        return new NYCTaxiRecord
        {
            VendorID = ParseInt(parts[0]),
            TpepPickupDatetime = ParseDateTime(parts[1]),
            TpepDropoffDatetime = ParseDateTime(parts[2]),
            PassengerCount = ParseInt(parts[3]),
            TripDistance = ParseDecimal(parts[4]),
            PickupLongitude = ParseDecimal(parts[5]),
            PickupLatitude = ParseDecimal(parts[6]),
            RateCodeID = ParseInt(parts[7]),
            StoreAndFwdFlag = parts[8],
            DropoffLongitude = ParseDecimal(parts[9]),
            DropoffLatitude = ParseDecimal(parts[10]),
            PaymentType = ParseInt(parts[11]),
            FareAmount = ParseDecimal(parts[12]),
            Extra = ParseDecimal(parts[13]),
            MtaTax = ParseDecimal(parts[14]),
            TipAmount = ParseDecimal(parts[15]),
            TollsAmount = ParseDecimal(parts[16]),
            ImprovementSurcharge = ParseDecimal(parts[17]),
            TotalAmount = ParseDecimal(parts[18]),
            SourceFileName = fileName
        };
    }
    catch
    {
        return null;
    }
}

static void AddRowToDataTable(DataTable dt, NYCTaxiRecord record)
{
    var row = dt.NewRow();
    row["VendorID"] = record.VendorID ?? (object)DBNull.Value;
    row["tpep_pickup_datetime"] = record.TpepPickupDatetime ?? (object)DBNull.Value;
    row["tpep_dropoff_datetime"] = record.TpepDropoffDatetime ?? (object)DBNull.Value;
    row["passenger_count"] = record.PassengerCount ?? (object)DBNull.Value;
    row["trip_distance"] = record.TripDistance ?? (object)DBNull.Value;
    row["pickup_longitude"] = record.PickupLongitude ?? (object)DBNull.Value;
    row["pickup_latitude"] = record.PickupLatitude ?? (object)DBNull.Value;
    row["RateCodeID"] = record.RateCodeID ?? (object)DBNull.Value;
    row["store_and_fwd_flag"] = record.StoreAndFwdFlag ?? (object)DBNull.Value;
    row["dropoff_longitude"] = record.DropoffLongitude ?? (object)DBNull.Value;
    row["dropoff_latitude"] = record.DropoffLatitude ?? (object)DBNull.Value;
    row["payment_type"] = record.PaymentType ?? (object)DBNull.Value;
    row["fare_amount"] = record.FareAmount ?? (object)DBNull.Value;
    row["extra"] = record.Extra ?? (object)DBNull.Value;
    row["mta_tax"] = record.MtaTax ?? (object)DBNull.Value;
    row["tip_amount"] = record.TipAmount ?? (object)DBNull.Value;
    row["tolls_amount"] = record.TollsAmount ?? (object)DBNull.Value;
    row["improvement_surcharge"] = record.ImprovementSurcharge ?? (object)DBNull.Value;
    row["total_amount"] = record.TotalAmount ?? (object)DBNull.Value;
    row["SourceFileName"] = record.SourceFileName ?? (object)DBNull.Value;
    dt.Rows.Add(row);
}

static async Task BulkInsertBatch(SqlConnection connection, DataTable dataTable)
{
    using var bulkCopy = new SqlBulkCopy(connection)
    {
        DestinationTableName = "Staging_NYCTaxi",
        BatchSize = 10000, // Wewnętrzny batch size dla SqlBulkCopy
        BulkCopyTimeout = 300
    };

    foreach (DataColumn column in dataTable.Columns)
    {
        bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
    }

    await bulkCopy.WriteToServerAsync(dataTable);
}

static int? ParseInt(string value)
{
    if (string.IsNullOrWhiteSpace(value)) return null;
    return int.TryParse(value, out var result) ? result : null;
}

static decimal? ParseDecimal(string value)
{
    if (string.IsNullOrWhiteSpace(value)) return null;

    value = value.Trim();

    // Jeśli wartość zaczyna się od kropki (np. ".70"), dodaj 0 na początku
    if (value.StartsWith("."))
    {
        value = "0" + value;
    }
    // Jeśli wartość zaczyna się od minus i kropki (np. "-.70"), wstaw 0 po minusie
    else if (value.StartsWith("-."))
    {
        value = "-0" + value.Substring(1);
    }

    return decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result) ? result : null;
}

static DateTime? ParseDateTime(string value)
{
    if (string.IsNullOrWhiteSpace(value)) return null;

    value = value.Trim();

    // Format daty w plikach NYC Taxi: "yyyy-MM-dd HH:mm:ss"
    var formats = new[]
    {
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd H:mm:ss",
        "yyyy-M-dd HH:mm:ss",
        "yyyy-M-d HH:mm:ss",
        "yyyy-MM-dd",
        "M/d/yyyy HH:mm:ss",
        "M/d/yyyy H:mm:ss"
    };

    if (DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
    {
        return result;
    }

    if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
    {
        return result;
    }

    return null;
}

class NYCTaxiRecord
{
    public int? VendorID { get; set; }
    public DateTime? TpepPickupDatetime { get; set; }
    public DateTime? TpepDropoffDatetime { get; set; }
    public int? PassengerCount { get; set; }
    public decimal? TripDistance { get; set; }
    public decimal? PickupLongitude { get; set; }
    public decimal? PickupLatitude { get; set; }
    public int? RateCodeID { get; set; }
    public string? StoreAndFwdFlag { get; set; }
    public decimal? DropoffLongitude { get; set; }
    public decimal? DropoffLatitude { get; set; }
    public int? PaymentType { get; set; }
    public decimal? FareAmount { get; set; }
    public decimal? Extra { get; set; }
    public decimal? MtaTax { get; set; }
    public decimal? TipAmount { get; set; }
    public decimal? TollsAmount { get; set; }
    public decimal? ImprovementSurcharge { get; set; }
    public decimal? TotalAmount { get; set; }
    public string? SourceFileName { get; set; }
}