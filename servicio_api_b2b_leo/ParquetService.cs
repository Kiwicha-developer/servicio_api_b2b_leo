//var builder = WebApplication.CreateBuilder(args);
//var app = builder.Build();

using Microsoft.AspNetCore.Server.Kestrel.Core;
using ParquetSharp;
using System.Diagnostics;
using System.Text.Json;

public class ParquetService : BackgroundService
{
    //declara la variable _logger para capturar los errores de la clase ParquetService
    private readonly ILogger<ParquetService> _logger;
    private static string _sesionFolder = "";

    public ParquetService(ILogger<ParquetService> logger)
    {
        _logger = logger;
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

        Dictionary<string, string> config;

        try
        {
            string rutaBase = AppContext.BaseDirectory;
            config = ReadConfiguration(Path.Combine(rutaBase, "config.txt"));
            _logger.LogInformation("Configuración cargada correctamente.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error al leer el archivo de configuración.");
            return;
        }

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

            var result = await UploadFilesAsync(form, $@"{config["FOLDER"]}");
            LogMemoryUsage("Fin de solicitud /upload");
            return result;
        });

        app.Urls.Add(config["URL"]);

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

            if (periodo == "auto")
            {
                periodo = periodo = DateTime.Now.ToString("yyyyMM");

            }

            string tempDir = _sesionFolder;

            if (int.Parse(chunkNumber) == 1)
            {
                tempDir = CreateTempFolderAsync(tempFolder, periodo);
            }
            

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

            

            if (int.Parse(chunkNumber) == int.Parse(totalChunks))
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
                    _sesionFolder = "";
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
                        var descriptor = reader.FileMetaData.Schema.Column(columnIndex);
                        bool isNullable = descriptor.MaxDefinitionLevel > 0;

                        Console.WriteLine($"Tipo Column: {columns[columnIndex].GetType()}");
                        Console.WriteLine($"Tipo Reader: {descriptor.PhysicalType}");
                        Console.WriteLine($"Tipo ReaderLogico: {descriptor.LogicalType}");

                        void TransferColumn<T>()
                        {
                                using var columnReader = rowGroupReader.Column(columnIndex).LogicalReader<T>();
                                var data = columnReader.ReadAll((int)rowGroupReader.MetaData.NumRows);
                                using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<T>();
                                columnWriter.WriteBatch(data);
                        }

                        // Manejo según tipo
                        if (columnDataType == typeof(string))
                        {
                            TransferColumn<string>();
                        }
                        else if (columnDataType == typeof(bool) || columnDataType == typeof(bool?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<bool?>();
                            }
                            else
                            {
                                TransferColumn<bool>();
                            }
                        }
                        else if (columnDataType == typeof(byte) || columnDataType == typeof(byte?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<byte?>();
                            }
                            else
                            {
                                TransferColumn<byte>();
                            }
                        }
                        else if (columnDataType == typeof(short) || columnDataType == typeof(short?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<short?>();
                            }
                            else
                            {
                                TransferColumn<short>();
                            }
                        }
                        else if (columnDataType == typeof(int) || columnDataType == typeof(int?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<int?>();
                            }
                            else
                            {
                                TransferColumn<int>();
                            }
                        }
                        else if (columnDataType == typeof(long) || columnDataType == typeof(long?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<long?>();
                            }
                            else
                            {
                                TransferColumn<long>();
                            }
                        }
                        else if (columnDataType == typeof(float) || columnDataType == typeof(float?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<float?>();
                            }
                            else
                            {
                                TransferColumn<float>();
                            }
                        }
                        else if (columnDataType == typeof(double) || columnDataType == typeof(double?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<double?>();
                            }
                            else
                            {
                                TransferColumn<double>();
                            }
                        }
                        else if (columnDataType == typeof(decimal) || columnDataType == typeof(decimal?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<decimal?>();
                            }
                            else
                            {
                                TransferColumn<decimal>();
                            }
                        }
                        else if (columnDataType == typeof(Guid) || columnDataType == typeof(Guid?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<Guid?>();
                            }
                            else
                            {
                                TransferColumn<Guid>();
                            }
                        }
                        else if (columnDataType == typeof(TimeSpan) || columnDataType == typeof(TimeSpan?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<TimeSpan?>();
                            }
                            else
                            {
                                TransferColumn<TimeSpan>();
                            }
                        }
                        else if (columnDataType == typeof(byte[]))
                        {
                            TransferColumn<byte[]>();
                        }
                        else if (columnDataType == typeof(ParquetSharp.Date) || columnDataType == typeof(ParquetSharp.Date?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<ParquetSharp.Date?>();
                            }
                            else
                            {
                                TransferColumn<ParquetSharp.Date>();
                            }
                        }

                        else if (columnDataType == typeof(DateTime) || columnDataType == typeof(DateTime?))
                        {
                            if (isNullable)
                            {
                                TransferColumn<DateTime?>();
                            }
                            else
                            {
                                TransferColumn<DateTime>();
                            }
                        }

                        else
                        {
                            Console.WriteLine($"Tipo no soportado: {columnDataType}");
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

            _sesionFolder = tempDir;
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
            Console.WriteLine($"LogicalType: {descriptor.LogicalType}");
            Console.WriteLine($"LogicalType entero: {descriptor.LogicalType.Type}");
            Console.WriteLine($"PhysicalType: {descriptor.PhysicalType}");
            Console.WriteLine($"IsNullable: {descriptor.MaxDefinitionLevel > 0}");

            bool isNullable = descriptor.MaxDefinitionLevel > 0;
            int precisionDefault = 18;
            int scaleDefault = 2;

            switch (descriptor.PhysicalType)
            {
                case PhysicalType.Boolean:
                    return isNullable ? new ParquetSharp.Column<bool?>(columnName)
                                        : new ParquetSharp.Column<bool>(columnName);

                case PhysicalType.Int32:
                    switch (descriptor.LogicalType.Type)
                    {
                        case LogicalTypeEnum.Date:
                            return isNullable
                                ? new ParquetSharp.Column<ParquetSharp.Date?>(columnName)
                                : new ParquetSharp.Column<ParquetSharp.Date>(columnName);

                        case LogicalTypeEnum.Int:
                            if (descriptor.LogicalType is IntLogicalType intType)
                            {
                                var bitWidth = intType.BitWidth;

                                if (bitWidth == 8)
                                {
                                    return isNullable
                                        ? new ParquetSharp.Column<byte?>(columnName)
                                        : new ParquetSharp.Column<byte>(columnName);
                                }
                                else if (bitWidth == 16)
                                {
                                    return isNullable
                                        ? new ParquetSharp.Column<short?>(columnName)
                                        : new ParquetSharp.Column<short>(columnName);
                                }
                            }

                            // Fallback a int
                            return isNullable
                                ? new ParquetSharp.Column<int?>(columnName)
                                : new ParquetSharp.Column<int>(columnName);

                        default:
                            return isNullable
                                ? new ParquetSharp.Column<int?>(columnName)
                                : new ParquetSharp.Column<int>(columnName);
                    }


                case PhysicalType.Int64:
                    switch (descriptor.LogicalType.Type)
                    {
                        case LogicalTypeEnum.Timestamp:
                            return isNullable
                                    ? new ParquetSharp.Column<DateTime?>(columnName)
                                    : new ParquetSharp.Column<DateTime>(columnName);
                        case LogicalTypeEnum.Time:
                            return isNullable
                                    ? new ParquetSharp.Column<TimeSpan?>(columnName)
                                    : new ParquetSharp.Column<TimeSpan>(columnName);
                        default:
                            return isNullable
                                ? new ParquetSharp.Column<long?>(columnName)
                                : new ParquetSharp.Column<long>(columnName);
                    }

                case PhysicalType.Int96:
                    return isNullable ? new ParquetSharp.Column<DateTime?>(columnName)
                                        : new ParquetSharp.Column<DateTime>(columnName);

                case PhysicalType.Float:
                    return isNullable
                        ? new ParquetSharp.Column<float?>(columnName)
                        : new ParquetSharp.Column<float>(columnName);

                case PhysicalType.Double:
                    return isNullable
                        ? new ParquetSharp.Column<double?>(columnName)
                        : new ParquetSharp.Column<double>(columnName);

                case PhysicalType.ByteArray:
                    return new ParquetSharp.Column<string>(columnName);

                case PhysicalType.FixedLenByteArray:
                    switch (descriptor.LogicalType.Type)
                    {
                        case LogicalTypeEnum.Decimal:
                            int precision = descriptor.TypePrecision > 0 ? descriptor.TypePrecision : precisionDefault;
                            int scale = descriptor.TypeScale > 0 ? descriptor.TypeScale : scaleDefault;
                            return isNullable
                                ? new ParquetSharp.Column<decimal?>(columnName, LogicalType.Decimal(precision, scale))
                                : new ParquetSharp.Column<decimal>(columnName, LogicalType.Decimal(precision, scale));

                        case LogicalTypeEnum.Uuid:
                            return isNullable
                                ? new ParquetSharp.Column<Guid?>(columnName,LogicalType.Uuid())
                                : new ParquetSharp.Column<Guid>(columnName, LogicalType.Uuid());

                        case LogicalTypeEnum.Interval:
                            return new ParquetSharp.Column<byte[]>(columnName);

                        default:
                            throw new NotSupportedException(
                                $"FixedLenByteArray con LogicalType '{descriptor.LogicalType.Type}' no está soportado aún para la columna '{columnName}'."
                            );
                    }

                case PhysicalType.Undefined:
                    _logger.LogError($"PhysicalType 'Undefined' no es válido para la columna '{columnName}'.");
                    throw new NotSupportedException($"PhysicalType 'Undefined' no es válido para la columna '{columnName}'.");

                default:
                    _logger.LogError($"LogicalType no soportado: {descriptor.LogicalType.Type}");
                    throw new NotSupportedException($"LogicalType no soportado: {descriptor.LogicalType.Type}");
            }

        }
        catch (Exception ex)
        {
            _logger.LogError($"Error al procesar la columna {columnName}: {ex.Message}");
            throw new NotSupportedException($"Tipo lógico no soportado: {descriptor.LogicalType.Type}", ex);
        }
    }


    private Dictionary<string, string> ReadConfiguration(string rutaArchivo)
    {
        var configuracion = new Dictionary<string, string>();

        foreach (string linea in File.ReadAllLines(rutaArchivo))
        {
            if (string.IsNullOrWhiteSpace(linea) || linea.StartsWith("#")) continue; // permite comentarios

            var partes = linea.Split('=', 2);
            if (partes.Length == 2)
            {
                string clave = partes[0].Trim();
                string valor = partes[1].Trim();
                configuracion[clave] = valor;
            }
        }

        return configuracion;
    }
}