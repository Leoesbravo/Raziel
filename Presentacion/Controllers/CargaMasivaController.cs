using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;

namespace Presentacion.Controllers
{
    public class CargaMasivaController : Controller
    {
        public IActionResult Carga()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Carga(IFormFile file)
        {
            var result = Negocio.Carga.ConvertToDataset(file);
            if (result.Item2 && result.Item4.Count == 0)
            {
                foreach (DataTable table in result.Item1.Tables)
                {
                    Negocio.Carga.BulkCopySql(table);
                }
                Dictionary<string, byte[]> archivosProcesados = GenerateCSVFiles(result.Item1);
                HttpContext.Session.SetString("Archivos", JsonConvert.SerializeObject(archivosProcesados));
                ViewBag.Result = 1;
                return View();

            }
            else if (result.Item4 != null)
            {
                ViewBag.Result = 2;
                return View(result.Item4);
            }
            else
            {
                ViewBag.Result = 3;
                return View();
            }

        }
        private byte[] GenerateProcessedCSV(DataTable table)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var streamWriter = new StreamWriter(memoryStream, Encoding.UTF8))
                {
                    streamWriter.WriteLine(string.Join(",", table.Columns.Cast<DataColumn>().Select(col => col.ColumnName)));

                    foreach (DataRow row in table.Rows)
                    {
                        string[] fields = row.ItemArray.Select(field => field.ToString()).ToArray();
                        streamWriter.WriteLine(string.Join(",", fields));
                    }
                }
                return memoryStream.ToArray();
            }
        }
        private Dictionary<string, byte[]> GenerateCSVFiles(DataSet dataSet)
        {
            Dictionary<string, byte[]> csvFiles = new Dictionary<string, byte[]>();

            foreach (DataTable table in dataSet.Tables)
            {
                byte[] csvData = GenerateProcessedCSV(table);
                csvFiles.Add(table.TableName + ".csv", csvData);
            }

            return csvFiles;
        }
        public ActionResult DescargarArchivos()
        {
            var archivosProcesados = HttpContext.Session.GetString("Archivos");
            var archivos = JsonConvert.DeserializeObject<Dictionary<string, byte[]>>(archivosProcesados);
            using (var memoryStream = new MemoryStream())
            {
                using (var zipArchive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                {
                    foreach (var archivo in archivos)
                    {
                        var zipEntry = zipArchive.CreateEntry(archivo.Key, CompressionLevel.Fastest);
                        using (var entryStream = zipEntry.Open())
                        {
                            entryStream.Write(archivo.Value, 0, archivo.Value.Length);
                        }
                    }
                }
                return File(memoryStream.ToArray(), "application/zip", "archivos_procesados.zip");
            }
        }
    }
}
