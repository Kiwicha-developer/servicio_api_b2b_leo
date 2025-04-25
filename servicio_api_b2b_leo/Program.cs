//Inicia el servicio usando la clase ParquetService
//Host.CreateDefaultBuilder(args)
//    .UseWindowsService()
//    .ConfigureServices((hostContext, services) =>
//    {
//        services.AddHostedService<ParquetService>();
//    })
//    .Build()
//    .Run();
using Serilog;

class Program
{
    public static void Main(string[] args)
    {
        // 1. Configura Serilog antes del builder
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

            // 2. Usa Serilog en el host builder
            Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .UseSerilog() // Esto integra Serilog al sistema de logging
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
            Log.CloseAndFlush(); // Asegura que todo se escriba en el archivo
        }
    }
}

