using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Negocio
{
    public class Carga
    {
        public static (DataSet, bool, string, List<string>) ConvertToDataset(IFormFile file)
        {
            List<string> errorList = new List<string>(); 
            DataSet dataSet = new DataSet();
            DataTable tableCharge = new DataTable();
            DataTable tableCompany = new DataTable();
            Dictionary<string, string> uniqueCompanies = new Dictionary<string, string>();
            int registro = 1;
            string[] formatos = { "MM-dd-yyyy", "dd-MM-yyyy", "yyyy-MM-dd" };
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
                        
                        string errores = "";
                        string[] rows = reader.ReadLine().Split(',');
                        if (rows.Length > 1)
                        {
                            registro = registro + 1;
                            DataRow dataRow = tableCharge.NewRow();

                            if (rows[0].Trim().Length > 10)
                            {
                                dataRow[0] = rows[0].Trim();
                            }
                            else
                            {
                                errores = errores + "No tiene ID, ";                               
                            }

                            dataRow[1] = rows[1].ToString().Trim();
                            if (rows[2].Trim().Length > 10)
                            {
                                dataRow[2] = rows[2].Trim();
                            }
                            else
                            {
                                errores = errores + "No tiene ID de compañia,";
                            }
                            rows[3] = Regex.Replace(rows[3], "[a-zA-Z]", "0");
                            dataRow[3] = decimal.Parse(rows[3].ToString().Trim());
                            dataRow[4] = rows[4].ToString();
                            if (formatos.Any(formato => DateTime.TryParseExact(rows[5].ToString(), formato, CultureInfo.InvariantCulture, DateTimeStyles.None, out _)))
                            {
                                dataRow[5] = rows[5].ToString();
                            }
                            else
                            {
                                errores = errores + "No tiene fecha de creación correcta,";
                            }
                            if( rows[6].ToString().Length > 0)
                            {
                                dataRow[6] = rows[6].ToString();
                            }
                            else
                            {
                                dataRow[6] = DBNull.Value;
                            }                           
                            tableCharge.Rows.Add(dataRow);
                            if (!uniqueCompanies.ContainsKey(rows[2]))
                            {
                                uniqueCompanies.Add(rows[2], rows[1]);
                            }
                            if(errores.Length > 0)
                            {
                                errores = "ID: " + registro + " " + errores;
                                errorList.Add(errores);
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
                tableCharge.Columns.RemoveAt(1);
                dataSet.Tables.Add(tableCompany);
                dataSet.Tables.Add(tableCharge);
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

        public static void BulkCopySql(DataTable table)
        {
            try
            {
                using (SqlConnection context = new SqlConnection(Data.Conexion.GetConnectionString()))
                {
                    context.Open();
                    using (SqlTransaction transaction = context.BeginTransaction())
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(context, SqlBulkCopyOptions.Default, transaction))
                        {
                            try
                            {
                                if(table.Columns.Count > 2)
                                {
                                    bulkCopy.DestinationTableName = "Charges";
                                }
                                else
                                {
                                    bulkCopy.DestinationTableName = "Companies";
                                }                             
                                bulkCopy.WriteToServer(table);
                                transaction.Commit();
                            }
                            catch (Exception)
                            {
                                transaction.Rollback();
                                context.Close();
                                throw;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }
}