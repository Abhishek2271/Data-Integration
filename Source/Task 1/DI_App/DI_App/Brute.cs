using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using LumenWorks.Framework.IO.Csv;
using System.IO;

namespace DI_App
{
    class Brute
    {
        private string FileLocation;   //File location of input file
        private string output_FileLocation;
        static int RowCount; 

        public Brute(string _FileLocation, string _output_FileLocation)
        {
            FileLocation = _FileLocation;
            output_FileLocation = _output_FileLocation;
        }

        public void StartMatching(int ReadLimit)
        {
            //Read the data to memory for fast comparisions.

            if (ReadLimit != 0)
            {
                RowCount = ReadLimit;
            }

            DataTable RT_Data = ReadCSVToMem(FileLocation, RowCount);
            
            FileMatching(RT_Data, output_FileLocation);
        }

        /// <summary>
        /// Brute force duplicate detection. Match the SSN column with any other column to detect the duplicate
        /// </summary>
        /// <param name="RT">Datatable containing input records (at least some part of it)</param>
        public void FileMatching(DataTable RT, string output_FileLocation)
        {
            Console.WriteLine("Start time: " + DateTime.Now);
            DateTime StartTime = DateTime.Now;
            var csv = new StringBuilder();
            int dupcount = 0;
            foreach (DataRow dr in RT.Rows)
            {
                //Loop through each data row
                foreach (DataRow di in RT.Rows)
                {
                    //DO NOT COMPARE WITH YOUR OWNSELF
                    if (di["RecID"].ToString() != dr["RecID"].ToString() && !string.IsNullOrEmpty(di["SSN"].ToString()))
                    {
                        // NO NEED TO CHECK THE COLUMN AGAIN IF IT IS ALREADY MATCHED
                        if (dr["SSN"].ToString() == di["SSN"].ToString())
                        {                           
                            dupcount++;
                            var newLine = string.Format("{0},{1}", dr["RecID"].ToString(), di["RecID"].ToString());
                            csv.AppendLine(newLine);
                        }
                    }
                    if (dupcount > 1)
                    {
                        if (!File.Exists(output_FileLocation))
                        {
                            File.WriteAllText(output_FileLocation, csv.ToString());
                        }
                        else
                            File.AppendAllText(output_FileLocation, csv.ToString());
                        dupcount = 0;
                        csv = new StringBuilder();                        
                    }                    
                }
            }
            if (!File.Exists(output_FileLocation))
            {
                File.WriteAllText(output_FileLocation, csv.ToString());
            }
            else
                File.AppendAllText(output_FileLocation, csv.ToString());

            Console.WriteLine(DateTime.Now);
            DateTime CompletionTime = DateTime.Now;
            var timeDiff = (CompletionTime - StartTime).TotalMinutes;
            Console.WriteLine("The total time taken is: " + timeDiff.ToString() + " Minutes.");
            Console.ReadLine();
        }


        #region ALL CSV Operations
        //TODO: Clean this up. Maybe create a utility class or something
        /// <summary>
        /// Load data from csv to Memory
        /// </summary>
        /// <returns>datatable with rows and columns read from CSV</returns>
        public DataTable ReadCSVToMem(string FileLocation, int RecordLimit)
        {
            int RowCount = 1;
            DataTable dt = new DataTable();
            List<string> RTHeaders = new List<string>();
            RTHeaders = GetCsvheader(FileLocation);

            foreach(string s in RTHeaders)
            {
                dt.Columns.Add(s);
            }

            CsvReader csv = new CsvReader(new StreamReader
                    (new FileStream(FileLocation, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)), true);
            while (csv.ReadNextRecord())
            {
                
                RowCount++;
                if (RowCount >= RecordLimit)
                {
                    return dt;
                }
                DataRow dr = dt.NewRow();
                foreach (string Header in RTHeaders)
                {
                    dr[Header] = csv[Header];
                }
                dt.Rows.Add(dr);
               
            }
            csv.Dispose();
            return dt;
        }     


        /// <summary>
        /// Gets the csvheader.
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private List<string> GetCsvheader(string FilePath)
        {
            using (CsvReader csvReader = new CsvReader
                (new StreamReader
                    (new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)), true))
            {

                csvReader.MissingFieldAction = MissingFieldAction.ReplaceByEmpty;
                return csvReader.GetFieldHeaders().ToList();
            }
        }
        #endregion
    }
}
