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

            try
            {
                using (StreamReader reader = new StreamReader(file.OpenReadStream()))
                {
                    DataTable tableCharge = CreateDataTable("Charge", reader.ReadLine());
                    DataTable tableCompany = new DataTable("Company");

                    Dictionary<string, string> uniqueCompanies = new Dictionary<string, string>();
                    int registro = 1;

                    while (!reader.EndOfStream)
                    {
                        string errores = "";
                        string[] rows = reader.ReadLine().Split(',');

                        if (rows.Length > 1)
                        {
                            DataRow dataRow = ProcessDataRow(tableCharge, rows, ref registro, ref errores);
                            if (!string.IsNullOrEmpty(errores))
                            {
                                errorList.Add($"ID: {registro} {errores}");
                            }

                            if (!uniqueCompanies.ContainsKey(rows[2]))
                            {
                                uniqueCompanies.Add(rows[2], rows[1]);
                            }
                        }
                    }

                    tableCompany = CreateCompanyDataTable(uniqueCompanies);

                    dataSet.Tables.Add(tableCompany);
                    dataSet.Tables.Add(tableCharge);
                }

                if (errorList.Count > 0)
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

        private static DataTable CreateDataTable(string tableName, string headers)
        {
            DataTable dataTable = new DataTable(tableName);
            string[] encabezados = headers.Split(',');

            foreach (string header in encabezados)
            {
                dataTable.Columns.Add(header.Trim());
            }

            return dataTable;
        }

        private static DataRow ProcessDataRow(DataTable tableCharge, string[] rows, ref int registro, ref string errores)
        {
            DataRow dataRow = tableCharge.NewRow();

            if (rows[0].Trim().Length > 10)
            {
                dataRow[0] = rows[0].Trim();
            }
            else
            {
                errores = "No tiene ID";
            }

            dataRow[1] = rows[1].Trim();

            if (rows[2].Trim().Length > 10)
            {
                dataRow[2] = rows[2].Trim();
            }
            else
            {
                errores = "No tiene ID de compañía";
            }

            rows[3] = Regex.Replace(rows[3], "[a-zA-Z]", "0");
            dataRow[3] = decimal.Parse(rows[3].Trim());
            dataRow[4] = rows[4].Trim();

            if (DateTime.TryParseExact(rows[5].Trim(), new string[] { "MM-dd-yyyy", "dd-MM-yyyy", "yyyy-MM-dd" }, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            {
                dataRow[5] = rows[5].Trim();
            }
            else
            {
                errores = "No tiene fecha de creación correcta";
            }

            if (rows[6].Trim().Length > 0)
            {
                dataRow[6] = rows[6].Trim();
            }
            else
            {
                dataRow[6] = DBNull.Value;
            }

            tableCharge.Rows.Add(dataRow);
            registro++;
            return dataRow;
        }

        private static DataTable CreateCompanyDataTable(Dictionary<string, string> uniqueCompanies)
        {
            DataTable tableCompany = new DataTable("Company");
            tableCompany.Columns.Add("CompanyID", typeof(string));
            tableCompany.Columns.Add("CompanyName", typeof(string));

            foreach (var company in uniqueCompanies)
            {
                DataRow companyRow = tableCompany.NewRow();
                companyRow["CompanyID"] = company.Key;
                companyRow["CompanyName"] = company.Value;
                tableCompany.Rows.Add(companyRow);
            }

            return tableCompany;
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