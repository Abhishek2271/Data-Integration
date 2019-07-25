using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Diagnostics;

/*
SUBMISSION BY: GROUP B
 * NAME: Abhishek Shrestha  (Matr. Nummer: 390055)
 *       Jia Jia            (Matr. Nummer: 389917)
 *       Syed Salman Ali    (Matr. Nummer: 395898)
*/


///<CreatedDate> 27 June 2018 </CreatedDate>
///<git> https://gitlab.tu-berlin.de/mandir123/DataIntegration</git>
/// <Summary>  
/// For the given dataset,
///             TASK 1. Use brute force for duplicate detection.
///             
/// </Summary>


namespace DI_App
{
    class Program
    {
        static string FileLocation = @"\InputDB.csv";       // Input source file
        static string OutputFileLocation = @"\Output.csv";  //output file
        static int sampleSize = 50000;                      //Limits the number of records read from csv file.

        static void Main(string[] args)
        {
            Assembly assem = Assembly.GetExecutingAssembly();
            string path = System.IO.Path.GetDirectoryName(assem.Location);
            Brute BruteForce = new Brute(path + FileLocation, path + OutputFileLocation);
            BruteForce.StartMatching(sampleSize);
        }
    }
}
