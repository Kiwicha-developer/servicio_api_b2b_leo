//var builder = WebApplication.CreateBuilder(args);
//var app = builder.Build();

using Apache.Arrow;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using ParquetSharp;
using ParquetSharp.Arrow;
using ParquetSharp.Schema;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Text.Json;

public class ParquetService : BackgroundService
{
    //declara la variable _logger para capturar los errores de la clase ParquetService
    private readonly ILogger<ParquetService> _logger;
    private readonly IHostApplicationLifetime _appLifetime;
    private string _upload_folder = "D:\\Trabajo_Grupo_Vega\\Archivos_Pruebas\\Temp_api_parque";
    //private string _upload_folder = "\\192.168.2.72\\p&g\\envio_Radar";

    public ParquetService(ILogger<ParquetService> logger, IHostApplicationLifetime appLifetime)
    {
        _logger = logger;
        _appLifetime = appLifetime;
    }

    //metodo para medir la memoria de los procesos
    private void LogMemoryUsage(string prefix = "")
    {
        var currentProcess = Process.GetCurrentProcess();
        var memoryUsedMB = currentProcess.WorkingSet64 / (1024.0 * 1024.0);
        _logger.LogInformation("{Prefix} Uso de memoria: {Memory:F2} MB", prefix, memoryUsedMB);
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogMemoryUsage("Servicio Inicalizado.");
        var builder = WebApplication.CreateBuilder();

        // Configuración del tamaño máximo de la solicitud
        builder.Services.Configure<KestrelServerOptions>(options =>
        {
            options.Limits.MaxRequestBodySize = 100 * 1024 * 1024; // 100 MB
        });
        var app = builder.Build();

        app.MapPost("/upload", async (HttpRequest request) =>
        {
            LogMemoryUsage("Inicio de solicitud /upload");
            var form = await request.ReadFormAsync();

            var result = await UploadFilesAsync(form, _upload_folder);
            LogMemoryUsage("Fin de solicitud /upload");
            return result;
        });

        app.Urls.Add("http://0.0.0.0:3025");

        LogMemoryUsage("Servidor HTTP iniciado dentro del servicio de Windows.");

        await app.RunAsync(stoppingToken);

        LogMemoryUsage("Servicio detenido");
    }

    private async Task<IResult> UploadFilesAsync (IFormCollection form,string tempFolder)
    {
        try
        {
            var file = form.Files["file"];
            string chunkNumber = form["chunk_number"].ToString();
            string totalChunks = form["total_chunks"].ToString();
            string periodo = form["periodo"].ToString();
            string tipo = form["tipo"].ToString();
            string _tempFolder = Path.Combine(tempFolder, "temp");

            if (file == null)
            {
                _logger.LogError("No se encontro el archivo en la solicitud");
                return Results.BadRequest(new { error = "El archivo no es un archivo Parquet válido" });
            }

            if (form.Files["file"] == null)
            {
                _logger.LogError("El archivo con clave 'file' no se encontró.");
                return Results.BadRequest(new { error = "El archivo no es un archivo Parquet válido" });
            }

            if (!file.FileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogError("Archivo no es formato Parquet");
                return Results.BadRequest(new { error = "El archivo no es un archivo Parquet válido" });
            }

            if (string.IsNullOrEmpty(chunkNumber) || string.IsNullOrEmpty(totalChunks) || string.IsNullOrEmpty(periodo))
            {
                _logger.LogError("Metadatos faltantes");
                return Results.BadRequest(new { error = "Faltan metadatos del chunk o periodo" });
            }

            string tempDir = CreateTempFolderAsync(tempFolder, periodo);

            if (string.IsNullOrWhiteSpace(tempDir))
            {
                _logger.LogError("Error al crear carpeta temporal");
                return Results.Problem("Error al crear carpeta temporal");
            }

            var chunkPath = Path.Combine(tempDir, $"chunk_{chunkNumber}.parquet");

            using (var stream = File.Create(chunkPath))
            {
                await file.CopyToAsync(stream);
            }

            var existingChunks = Directory.GetFiles(tempDir, "*.parquet");

            if (existingChunks.Length == int.Parse(totalChunks))
            {
                LogMemoryUsage("Iniciando Fusion de Chunks");
                List<string> orderChunks = existingChunks.OrderBy(f => int.Parse(Path.GetFileNameWithoutExtension(f).Split('_')[1])).ToList();
                string finalFilePath = Path.Combine(tempFolder, $"{periodo}{(string.IsNullOrEmpty(tipo) ? "" : "-" + tipo)}.parquet");

                try
                {
                    // Realizar la fusión
                    MergeParquetFiles(orderChunks, finalFilePath);
                    // Limpieza de archivos temporales
                    DeleteTempFolderAsync(orderChunks);

                    return Results.Ok(new { message = "Todos los chunks recibidos y fusionados correctamente", final_file = finalFilePath });
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error en el proceso de fusión: {ex.Message}");
                    return Results.Problem("Error al fusionar los archivos");
                }
            }
            _logger.LogInformation($"Chunk {chunkNumber} de {totalChunks} recibido exitosamente");
            return Results.Ok(new{ message = $"Chunk {chunkNumber} de {totalChunks} recibido exitosamente"});
            
        }
        catch (Exception ex)
        {
            _logger.LogError("Error en el proceso de fusion");
            return Results.Problem(ex.Message);
        }
        
    }

    private void MergeParquetFiles(List<string> chunkFiles, string outputFile)
    {
        LogMemoryUsage("Inicio de Fusion");
        try
        {
            //Obtenemos el primer file y obtenemos el esquema a travez de un metodo personalizado
            using var readerFile = new ParquetFileReader(chunkFiles[0]);

            ParquetSharp.Column[] columns= GetColumns(readerFile);

            using var outputWriter = new ParquetFileWriter(outputFile, columns);

            foreach (var chunkFile in chunkFiles)
            {
                using var reader = new ParquetFileReader(chunkFile);

                for (int rowGroupIndex = 0; rowGroupIndex < reader.FileMetaData.NumRowGroups; rowGroupIndex++)
                {
                    using var rowGroupReader = reader.RowGroup(rowGroupIndex);
                    using var rowGroupWriter = outputWriter.AppendRowGroup();

                    for (int columnIndex = 0; columnIndex < columns.Length; columnIndex++)
                    {
                        var columnDataType = columns[columnIndex].GetType().GenericTypeArguments[0];

                        if (columnDataType == typeof(string))
                        {
                            using var columnReader = rowGroupReader.Column(columnIndex).LogicalReader<string>();
                            var data = columnReader.ReadAll((int)rowGroupReader.MetaData.NumRows);
                            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<string>();
                            columnWriter.WriteBatch(data);
                        }
                        else if (columnDataType == typeof(int) || columnDataType == typeof(int?))
                        {
                            using var columnReader = rowGroupReader.Column(columnIndex).LogicalReader<int?>();
                            var data = columnReader.ReadAll((int)rowGroupReader.MetaData.NumRows);
                            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<int?>();
                            columnWriter.WriteBatch(data);
                        }
                        else if (columnDataType == typeof(long) || columnDataType == typeof(long?))
                        {
                            using var columnReader = rowGroupReader.Column(columnIndex).LogicalReader<long?>();
                            var data = columnReader.ReadAll((int)rowGroupReader.MetaData.NumRows);
                            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<long?>();
                            columnWriter.WriteBatch(data);
                        }
                        else if (columnDataType == typeof(double) || columnDataType == typeof(double?))
                        {
                            using var columnReader = rowGroupReader.Column(columnIndex).LogicalReader<double?>();
                            var data = columnReader.ReadAll((int)rowGroupReader.MetaData.NumRows);
                            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<double?>();
                            columnWriter.WriteBatch(data);
                        }
                        else if (columnDataType == typeof(decimal) || columnDataType == typeof(decimal?))
                        {
                            using var columnReader = rowGroupReader.Column(columnIndex).LogicalReader<decimal?>();
                            var data = columnReader.ReadAll((int)rowGroupReader.MetaData.NumRows);
                            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal?>();
                            columnWriter.WriteBatch(data);
                        }
                        else
                        {
                            throw new NotSupportedException($"Tipo no soportado: {columnDataType}");
                        }
                    }
                }
            }

            outputWriter.Close(); // Finaliza correctamente el archivo Parquet
            LogMemoryUsage("Fusión completada y archivo final generado: \" + outputFile");

        }
        catch (Exception ex)
        {
            _logger.LogError($"Error en la fusión de Parquet: {ex.Message}");
            throw;
        }
    }

    private string CreateTempFolderAsync(string tempFolder,string periodo)
    {
        try
        {
            string _tempFolder = Path.Combine(tempFolder, "temp");
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var tempDir = Path.Combine(_tempFolder, $"{periodo}_{timestamp}");

            Directory.CreateDirectory(tempDir);

            LogMemoryUsage($"Carpeta temporal {tempDir} creada");
            return tempDir;
        }
        catch (Exception ex)
        {
            _logger.LogError("Error al crear carpeta temporal" + ex);
            return string.Empty;
        }
    }

    private void DeleteTempFolderAsync(List<string> orderChunks)
    {
        foreach (var chunkFile in orderChunks)
        {
            try
            {
                File.Delete(chunkFile);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error al eliminar el directorio temporal {chunkFile}: {ex.Message}");
            }
        }

        LogMemoryUsage("Directorios temporales eliminados correctamente");
    }

    //metodo para borrar carpeta entera
    //private void DeleteTempFolderAsync(string folder)
    //{
    //    try
    //    {
    //        Directory.Delete(folder, recursive: true);

    //        _logger.LogInformation($"Directorio temporal {folder} eliminado.");
    //    }
    //    catch (Exception ex)
    //    {
    //        _logger.LogError($"Error al eliminar el directorio temporal {folder}: {ex.Message}");
    //    }
    //}

    //Metodo para optener el esquema de los documentos parquet a travez de la clase COLUMN
    private ParquetSharp.Column[] GetColumns(ParquetFileReader fileReader)
    {
        List<ParquetSharp.Column> columns = new List<ParquetSharp.Column>();
        try
        {
            //obtenemos 
            int numColumns = fileReader.FileMetaData.NumColumns;

            SchemaDescriptor schema = fileReader.FileMetaData.Schema;
            for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex)
            {
                ColumnDescriptor descriptor = schema.Column(columnIndex);
                string columnName = descriptor.Name;
                columns.Add(GetColumnType(descriptor, columnName));
            }

            LogMemoryUsage("Esquema obtenido exitosamente");
            return columns.ToArray();
        }
        catch(Exception ex)
        {
            _logger.LogError("Error al obtener esquema" + ex);
            return columns.ToArray();
        }
        
    }

    //Metodo para obtener el Type del esquema
    private ParquetSharp.Column GetColumnType(ColumnDescriptor descriptor, string columnName)
    {
        try
        {
            int precisionDefault = 18;
            int scaleDefault = 2;

            switch ((int)descriptor.PhysicalType)
            {
                case 0: // BOOLEAN
                    return new ParquetSharp.Column<bool?>(columnName);

                case 1: // INT32
                    return new ParquetSharp.Column<int?>(columnName);

                case 2: // INT64
                    return new ParquetSharp.Column<long?>(columnName);

                case 3: // INT96
                        // long o byte[]
                    return new ParquetSharp.Column<byte[]>(columnName);

                case 4: // FLOAT
                    return new ParquetSharp.Column<float?>(columnName);

                case 5: // DOUBLE
                    return new ParquetSharp.Column<double?>(columnName);

                case 7: // FIXED_LEN_BYTE_ARRAY
                    return new ParquetSharp.Column<byte[]>(columnName);
                case 6: // BYTE_ARRAY (STRING)
                    return new ParquetSharp.Column<string>(columnName);

                case 8: // DATE
                    return new ParquetSharp.Column<DateTime?>(columnName);

                case 9: // TIME_MILLIS
                    return new ParquetSharp.Column<TimeSpan?>(columnName);

                case 10: // TIMESTAMP_MILLIS
                    return new ParquetSharp.Column<DateTime?>(columnName);

                case 11: // TIMESTAMP_MICROS
                    return new ParquetSharp.Column<DateTime?>(columnName);

                case 12: // DECIMAL
                         // Para Decimal, hay que especificar la precisión y la escala
                    return new Column<decimal?>(columnName, LogicalType.Decimal(
                                                    precision: descriptor.TypePrecision > 0 ? descriptor.TypePrecision : precisionDefault,
                                                    scale: descriptor.TypeScale > 0 ? descriptor.TypeScale : scaleDefault
                                                ));

                case 13: // UINT_8
                    return new ParquetSharp.Column<byte>(columnName);

                case 14: // UINT_16
                    return new ParquetSharp.Column<ushort>(columnName);

                case 15: // UINT_32
                    return new ParquetSharp.Column<uint>(columnName);

                case 16: // UINT_64
                    return new ParquetSharp.Column<ulong>(columnName);

                case 17: // JSON
                    return new ParquetSharp.Column<string>(columnName);

                case 18: // UUID
                    return new ParquetSharp.Column<Guid>(columnName);

                case 19: // INTERVAL
                    return new ParquetSharp.Column<TimeSpan>(columnName);

                case 20: // LIST
                    return new ParquetSharp.Column<string[]>(columnName);

                case 21: // MAP
                    return new ParquetSharp.Column<Dictionary<string, object>>(columnName);

                case 22: // STRUCT
                    return new ParquetSharp.Column<object>(columnName);

                case 23: // UNION
                    return new ParquetSharp.Column<object>(columnName);

                case 24: // ENUM
                    return new ParquetSharp.Column<int>(columnName);
                default:
                    throw new NotSupportedException($"Tipo lógico no soportado: {descriptor.LogicalType.Type}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("Error en obtener objeto Column" + ex);
            throw new NotSupportedException($"Tipo lógico no soportado: {descriptor.LogicalType.Type}");
        }
        
    }
}