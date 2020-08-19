using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.IO;
using System.Data;
using MySql.Data.MySqlClient;
using System.Linq;

namespace DSMR_To_DB
{ 
    class Program
    {
        static void Main(string[] args)
        {
            WebRequest request = WebRequest.Create("http://dsmr-api.local/api/v1/hist/hours");
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();

            Stream dataStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(dataStream);
            string jsonResponse = reader.ReadToEnd();

            Console.WriteLine("Json-Package successfully received: \n");
            Console.WriteLine(jsonResponse);

            Console.WriteLine("\n\nConverting package to object:");
            
            Packages.Hours package = JsonConvert.DeserializeObject<Packages.Hours>(jsonResponse);
            package.hours.RemoveAt(0);


            Console.WriteLine("Package-Object successfully build");
            Console.WriteLine("\nEstablishing connection to SQL Database:");

            string connectionString = null;

            foreach (string argument in args)
            {
                if (argument.IndexOf("/p:ConnectionString=") == 0)
                {                    
                    connectionString = argument.Remove(0, 20);                    
                }
            }

            if (connectionString == null)
            {
                connectionString = @"Server=" + Config.Server + ";Port=" + Config.Port + ";Database=" + Config.Database + ";User ID=" + Config.User_ID + ";Password=" + Config.Password;
            }

            Console.WriteLine("Using ConnectionString " + connectionString);
            MySqlConnection mySqlConnection = new MySqlConnection(connectionString);

            try
            {
                mySqlConnection.Open();
            } catch (Exception ex)
            {
                Console.WriteLine("A terminal error occured while attemtping to connect to the specified SQL database. The programm will be terminated:");
                Console.WriteLine(ex.Message + "\n\n\n");
                return;
            }

            Console.WriteLine("SQL Connection Established!");
            Console.WriteLine("\nInserting data into SQL database");

            int successCount = 0;
            int failCount = 0;
            foreach (Packages.Hour hour in package.hours)
            {
                string querryString = "INSERT INTO `hourly`(`timestamp`, `edt1`, `edt2`, `ert1`, `ert2`, `gdt`) VALUES (" + hour.recid + "," + hour.edt1.ToString().Replace(',', '.') + "," + hour.edt2.ToString().Replace(',', '.') + "," + hour.ert1.ToString().Replace(',', '.') + "," + hour.ert2.ToString().Replace(',', '.') + "," + hour.gdt.ToString().Replace(',', '.') + ")";
                MySqlCommand querry = new MySqlCommand(querryString, mySqlConnection);
                MySqlDataReader dataReader = null;
                try
                {
                    dataReader = querry.ExecuteReader();
                } catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    if (ex.Message.Contains("Duplicate entry"))
                    {
                        Console.WriteLine("Entry for timestamp " + hour.recid + " already found. Entries are up to date: " + ex.Message);
                        break;
                    }
                    Console.WriteLine("An error occured while inserting timestamp" + hour.recid + ". Entry will be skipped: " + ex.Message);
                    failCount++;
                    continue;
                } finally
                {
                    if (dataReader != null)
                    {
                        dataReader.Close();
                    }
                }
                Console.WriteLine("Entry for timestamp " + hour.recid + " successfully added");
                successCount++;
            }

            Console.WriteLine("\n" + successCount + " out of 48 rows successfully added, " + failCount + " entries failed, " + (48 - failCount - successCount) + " entries up to date");

            if (args.Contains("-s"))
            {
                Console.ReadKey();
            }
        }
    }
}
