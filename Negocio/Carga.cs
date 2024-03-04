using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Negocio
{
    public class Carga
    {
        public static (DataSet, bool, string, List<object>) ConvertToDataset(IFormFile file)
        {
            List<object> errorList = new List<object>(); 
            DataSet dataSet = new DataSet();
            DataTable tableCharge = new DataTable();
            DataTable tableCompany = new DataTable();
            Dictionary<string, string> uniqueCompanies = new Dictionary<string, string>();
            try
            {
                using (StreamReader reader = new StreamReader(file.OpenReadStream()))
                {
                    string[] encabezados = reader.ReadLine().Split(',');

                    foreach (string header in encabezados)
                    {
                        tableCharge.Columns.Add(header.Trim());
                    }

                    while (!reader.EndOfStream)
                    {
                        string[] rows = reader.ReadLine().Split(',');
                        if (rows.Length > 1)
                        {
                            DataRow dataRow = tableCharge.NewRow();

                            if (rows[0].Trim().Length > 10)
                            {
                                dataRow[0] = rows[0].Trim();
                            }
                            else
                            {
                                errorList.Add(dataRow);
                            }
                            dataRow[1] = rows[1].ToString().Trim();
                            dataRow[2] = rows[2].ToString().Trim();
                            rows[3] = Regex.Replace(rows[3], "[a-zA-Z]", "0");
                            dataRow[3] = decimal.Parse(rows[3].ToString().Trim());
                            dataRow[4] = rows[4].ToString();
                            dataRow[5] = rows[5].ToString();
                            dataRow[6] = rows[6].ToString();

                            tableCharge.Rows.Add(dataRow);
                            if (!uniqueCompanies.ContainsKey(rows[2]))
                            {
                                uniqueCompanies.Add(rows[2], rows[1]);
                            }
                        }
                    }
                    tableCompany.Columns.Add("CompanyID", typeof(string));
                    tableCompany.Columns.Add("CompanyName", typeof(string));

                    foreach (var company in uniqueCompanies)
                    {
                        DataRow companyRow = tableCompany.NewRow();
                        companyRow["CompanyID"] = company.Key;
                        companyRow["CompanyName"] = company.Value;
                        tableCompany.Rows.Add(companyRow);
                    }
                }
                dataSet.Tables.Add(tableCharge);
                dataSet.Tables.Add(tableCompany);
                if(errorList.Count > 0)
                {
                    return (dataSet, false, "", errorList);
                }
                else
                {
                    return (dataSet, true, "", null);
                }            
            }
            catch (Exception ex)
            {
                return (dataSet, false, ex.Message, null);
            }
            
        }

        public static Modelo.Result BulkCopySql(IFormFile file)
        {
            Modelo.Result result = new Modelo.Result();
            try
            {
                //Leemos el archivo y creamos nuestro datatable
                DataTable DT = CSVtoDT(file);

                //Lo mandamos a la base de datos
                using (SqlConnection context = new SqlConnection(Data.Conexion.GetConnectionString()))
                {
                    context.Open();
                    using (SqlTransaction transaction = context.BeginTransaction())
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(context, SqlBulkCopyOptions.Default, transaction))
                        {
                            try
                            {
                                bulkCopy.DestinationTableName = "Cargo";
                                bulkCopy.WriteToServer(DT);
                                transaction.Commit();
                                result.Correct = true;
                            }
                            catch (Exception)
                            {
                                transaction.Rollback();
                                context.Close();
                                result.Correct = false;
                                throw;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result.Correct = false;
                result.ErrorMessage = ex.Message;
            }
            return result;
        }

        public static DataTable CSVtoDT(IFormFile file)
        {
            //Se crea el DT
            DataTable dt = new DataTable();
            try
            {
                using (StreamReader reader = new StreamReader(file.OpenReadStream()))
                {
                    //Creamos el header del DT
                    string[] encabezados = reader.ReadLine().Split(',');

                    //Agregamos la columnas al DT
                    foreach (string header in encabezados)
                    {
                        dt.Columns.Add(header.Trim());
                    }

                    //Se leen las demas lineas y las agrega al DT
                    while (!reader.EndOfStream)
                    {
                        string[] rows = reader.ReadLine().Split(',');
                        if (rows.Length > 1)
                        {
                            DataRow dataRow = dt.NewRow();

                            rows[3] = Regex.Replace(rows[3], "[a-zA-Z]", "0");

                            dataRow[0] = rows[0].ToString().Trim();
                            dataRow[1] = rows[1].ToString().Trim();
                            dataRow[2] = rows[2].ToString().Trim();
                            dataRow[3] = decimal.Parse(rows[3].ToString().Trim());
                            dataRow[4] = rows[4].ToString();
                            //dataRow[5] = rows[5].ToString();
                            if (DateTime.TryParseExact(rows[5], "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
                            {
                                dataRow[5] = date; // Asignar la fecha parseada
                            }
                            else
                            {
                                dataRow[5] = rows[5].ToString();
                            }
                            //dataRow[6] = rows[6].ToString();
                            // Validar y asignar valores para la séptima columna
                            if (!string.IsNullOrWhiteSpace(rows[6]))
                            {
                                dataRow[6] = rows[6].ToString().Trim();
                            }
                            else
                            {
                                dataRow[6] = DBNull.Value; // Usar DBNull.Value para valores vacíos
                            }

                            dt.Rows.Add(dataRow);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error al leer el archivo CSV: " + ex.Message);
            }
            return dt;
        }
    }
}