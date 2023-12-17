using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;



class PingPong
{
    static string tabel = "Sales.OrderTracking";
    static int Menu() 
    {
        int answer;

        Console.WriteLine("Choose what action do you want to do:");
        Console.WriteLine("1. Find order ...");

        answer = Convert.ToInt32(Console.ReadLine());

        return answer;
    }

    static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {

            if (comm.Rank == 0)
            {
                int operation = Menu();
                SqlConnection cnn = SqlConnect();

                switch (operation) 
                {
                    case 1:
                        TopSearchItem(cnn, comm);
                        break;
                    default:

                        break;
                }

              

                for (int dest = 1; dest < comm.Size; ++dest)
                {
                    comm.Send("MPI.Kill", dest, 0);
                }

                SequentalSearch("qwe");

            }
            else
            {
                Parallel(comm);
            }
        });

    }

    static void Parallel(MPI.Intracommunicator comm)
    {
        Console.WriteLine("Waiting command...");
        int command = comm.Receive<int>(0, 0);

        switch (command)
        {
            case 1:
                SubSearchForItem(comm);
                break;
        }

    }

    static void TopSearchItem(SqlConnection cnn, MPI.Intracommunicator comm) 
    {
        List<string> subElements;
        string searchTarget = "790 Shelbyville Road";
        string query = $"SELECT CarrierTrackingNumber from {tabel}";


        for (int dest = 1; dest < comm.Size; ++dest) 
        {
            comm.Send(1, dest, 0);
        }
        Console.WriteLine("sent packets");

        List<string> elements = MakeRequest(cnn, query);
        cnn.Close();

        int mpiSize = comm.Size - 1;
        int portionSize = elements.Count / mpiSize;

        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(searchTarget, dest, 0);

            if (dest != (comm.Size - 1))
            {
                subElements = elements.GetRange((dest - 1) * portionSize, portionSize);
                comm.Send(subElements, dest, 0);
            }
            else
            {
                subElements = elements.GetRange((dest - 1) * portionSize, elements.Count - ((dest - 1) * portionSize));
                comm.Send(subElements, dest, 0);
            }

        }

        for (int i = 1; i < comm.Size; i++)
        {
            Console.WriteLine(comm.Receive<string>(i, 1));
        }
    }


    static void SubSearchForItem(MPI.Intracommunicator comm)
    {
        while (true)
        {
            List<string> part;
            string searchTarget;
            string answer = "Not found";

            Console.WriteLine("Waiting search target...");
            searchTarget = comm.Receive<string>(0, 0);

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            if (searchTarget == "MPI.Kill")
            {
                break;
            }

            part = comm.Receive<List<string>>(0, 0);

            foreach (string item in part)
            {
                if (item == searchTarget)
                {
                    answer = item;
                }
            }

            comm.Send(answer, 0, 1);

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            ts.Hours, ts.Minutes, ts.Seconds,
            ts.Milliseconds / 10);

            Console.WriteLine("Task was completed within: {0}", elapsedTime);

            //Console.WriteLine("{0} process completed task! with size {1}", MPI.Environment.HostRank, part.Count);
            //zConsole.WriteLine("Last element: {0}", part.Last());
        }


    }


    static void SequentalSearch(string searchTarget)
    {
        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        string query = $"SELECT CarrierTrackingNumber from {tabel}";

        SqlConnection cnn = SqlConnect();
        List<string> elements = MakeRequest(cnn, query);
        cnn.Close();

        string answer = "Not found";

        foreach (string item in elements)
        {
            if (item == searchTarget)
            {
                answer = item;
            }
        }



        stopWatch.Stop();
        TimeSpan ts = stopWatch.Elapsed;
        string elapsedTime = String.Format("Sequental: {0:00}:{1:00}:{2:00}.{3:00}",
        ts.Hours, ts.Minutes, ts.Seconds,
        ts.Milliseconds / 10);

        Console.WriteLine(elapsedTime);

    }

    static SqlConnection SqlConnect()
    {
        string connectionString;
        SqlConnection cnn;
        connectionString = "Data Source=GEORGE\\SQLEXPRESS;Initial Catalog=AdventureWorks2016_EXT;Integrated Security=true;TrustServerCertificate=true";
        cnn = new SqlConnection(connectionString);
        cnn.Open();

        return cnn;
    }

    static List<string> MakeRequest(SqlConnection cnn, string query)
    {
        SqlCommand command = new SqlCommand(query, cnn);
        SqlDataReader reader = command.ExecuteReader();

        List<string> elements = new List<string>(2048);
        while (reader.Read())
        {
            elements.Add((string)reader.GetValue(0));
        }

        return elements;
    }
}