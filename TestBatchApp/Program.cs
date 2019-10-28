using Microsoft.Azure.Batch.Conventions.Files;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace testbatchapp
{
    public class Program
    {

        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting testbatchapp exe in Cloud Task");            
            string taskId = Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID");

            // Obtain the custom environment variable we set in the client application
            string jobContainerUrl = Environment.GetEnvironmentVariable("JOB_CONTAINER_URL");
            TaskOutputStorage taskStorage = new TaskOutputStorage(new Uri(jobContainerUrl), taskId);

            string index = args[0];
            string storageAccountName = "emergentai";
            string storageAccountKey = "FqOL31fHPNRhRW3jpLAeL8FF+sfyZqYEtUqzMpJo257CCSIleJKusGihiNxOqqwV4b/JSq+srU23S5C2Ao9dRw==";

            // open the cloud blob that contains the input file
            int number = ReadDataFromBlobStorage(index, storageAccountName, storageAccountKey);

            // add some data to the loaded input file
            number = taskETL(number);

            // write back to blob ready for next task
            WriteDataToBlobStorage(index, number, storageAccountName, storageAccountKey);

            Thread.Sleep(5000);

            // finally output to task storage for exit data reporting
            await WriteFinalOutput(number, taskId, taskStorage, index);

            Thread.Sleep(5000);

            Console.WriteLine("Finishing testbatchapp Cloud Task");
        }

        private static int taskETL(int number)
        {
            number += 1;
            return number;
        }

        private static int ReadDataFromBlobStorage(string index, string storageAccountName, string storageAccountKey)
        {
            int pos = int.Parse(index);
            int detent = pos - 1;
            string blobName =  pos == 0 ? "https://emergentai.blob.core.windows.net/input/input.txt" : $"https://emergentai.blob.core.windows.net/input/input{detent}.txt";
            Console.WriteLine("LoadInputFromBlobStorage start");
            Console.WriteLine($"blobname {blobName}");
            var storageCred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(blobName), storageCred);           
            int number = 0;
            using (Stream memoryStream = new MemoryStream())
            {
                blob.DownloadToStream(memoryStream);
                memoryStream.Position = 0;
                using (var sr = new StreamReader(memoryStream))
                {
                    var myStr = sr.ReadToEnd();
                    number = int.Parse(myStr);
                }
                Console.WriteLine("LoadInputFromBlobStorage finish");
            }
            return number;
        }

        private static void WriteDataToBlobStorage(string index, int data, string storageAccountName, string storageAccountKey)
        {
            int pos = int.Parse(index);           
            string blobName = $"https://emergentai.blob.core.windows.net/input/input{pos}.txt";
            Console.WriteLine("WriteDataToBlobStorage start");
            Console.WriteLine($"blobname {blobName}");
            var storageCred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(blobName), storageCred);
            using (Stream memoryStream = new MemoryStream())
            {                
                string wordline = data.ToString();
                byte[] dataToWrite = Encoding.UTF8.GetBytes(wordline);
                memoryStream.Write(dataToWrite, 0, dataToWrite.Length);
                memoryStream.Position = 0;
                blob.UploadFromStream(memoryStream);

                Console.WriteLine("WriteDataToBlobStorage finish");
            }
        }

        private static async Task WriteFinalOutput(int number, string taskId, TaskOutputStorage taskStorage, string index)
        {
            Console.WriteLine("Write output to task storage from EXE");
            using (ITrackedSaveOperation stdout = await taskStorage.SaveTrackedAsync(
                TaskOutputKind.TaskLog,
                RootDir("stdout.txt"),
                "stdout.txt",
                TimeSpan.FromSeconds(15)))
            {
                Console.WriteLine("Dump output to file from EXE - start");

                string outputFile = "results.txt"; 

                using (StreamWriter output = File.CreateText(WorkingDir(outputFile)))
                {
                    output.WriteLine($"final task {taskId}");
                    output.WriteLine($"element: {number}");
                }

                // Persist the task output to Azure Storage
                Task.WaitAll(taskStorage.SaveAsync(TaskOutputKind.TaskOutput, outputFile));

                // We are tracking the disk file to save our standard output, but the node agent may take
                // up to 3 seconds to flush the stdout stream to disk. So give the file a moment to catch up.
                await Task.Delay(stdoutFlushDelay);

                Console.WriteLine("Dump output to file from EXE - finish");
            }
        }

        private static string RootDir(string path)
        {
            return Path.Combine(Environment.GetEnvironmentVariable("AZ_BATCH_TASK_DIR"), path);
        }

        private static string WorkingDir(string path)
        {
            return Path.Combine(Environment.GetEnvironmentVariable("AZ_BATCH_TASK_WORKING_DIR"), path);
        }

        private static readonly TimeSpan stdoutFlushDelay = TimeSpan.FromSeconds(3);
    }
}
