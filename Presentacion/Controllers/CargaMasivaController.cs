using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.IO;
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
            if (result.Item2 && result.Item3 == null)
            {
                Dictionary<string, byte[]> archivosProcesados = GenerateCSVFiles(result.Item1);
                foreach (var archivo in archivosProcesados)
                {
                    ViewBag.Mensaje = "Se ha procesado tu informacion satisfactoriamente";
                    return File(archivo.Value, "text/csv", archivo.Key);
                }
            }
            else if(result.Item3 != null)
            {
                ViewBag.Mensaje = "Hay registros que no cumplen con el formato necesario";
                return View();
            }
            else
            {
                ViewBag.Mensaje = "Ocurrio un error al procesas tu archivo";
                return View();
            }
            return View();
           
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
        [HttpGet]
        public IActionResult BulkCopy()
        {
            return View();
        }
        [HttpPost]
        public IActionResult BulkCopy(Modelo.Result result)
        {
            IFormFile file = Request.Form.Files["csv"];

            Modelo.Result result1 = Negocio.Carga.BulkCopySql(file);

            return View(result1);
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
    }
}
