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
                            if (rows[5].ToString().Contains("/"))
                            {
                                dataRow[5] = rows[5].ToString();
                            }
                            else
                            {
                                dataRow[5] = DateTime.Now.ToString();   
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