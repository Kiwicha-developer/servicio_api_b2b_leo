
using Serilog;

class Program
{
    public static void Main(string[] args)
    {
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.File(
                path: "C:\\b2b\\flask_service.log",
                rollingInterval: RollingInterval.Day,
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {SourceContext} - {Message:lj}{NewLine}{Exception}"
            )
            .CreateLogger();

        try
        {
            Log.Information("Iniciando el servicio...");

            Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .UseSerilog() 
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<ParquetService>();
                })
                .Build()
                .Run();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "El servicio terminó de forma inesperada");
        }
        finally
        {
            Log.CloseAndFlush(); 
        }
    }
}

