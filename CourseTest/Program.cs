using Microsoft.Data.SqlClient;
using System.Diagnostics;

class PingPong
{
    static string tabel = "Sales.OrderTracking";
    static string tabelPasswords = "Person.Password";
    static int Menu()
    {
        int answer;

        Console.WriteLine("Choose what action do you want to do:");
        Console.WriteLine("1. Find order with ID = 55A8E43-A1C1-4320-B6F9-6A");
        Console.WriteLine("2. Get hash of table");
        Console.WriteLine("3. Sort id of orders");

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
                        string searchTarget = "55A8E43-A1C1-4320-BDF9-6A";
                        TopSearchItem(cnn, comm, searchTarget);
                        SequentalSearch(searchTarget);
                        break;
                    case 2:
                        TopHash(cnn, comm);
                        SequentalHash();
                        break;
                    case 3:
                        TopSorting(cnn, comm);
                        SequentalSorting();
                        break;
                }



                for (int dest = 1; dest < comm.Size; ++dest)
                {
                    comm.Send("MPI.Kill", dest, 0);
                }


            }
            else
            {
                Parallel(comm);
            }
        });

    }

    static void Parallel(MPI.Intracommunicator comm)
    {
        int command = comm.Receive<int>(0, 0);

        switch (command)
        {
            case 1:
                SubSearchForItem(comm);
                break;
            case 2:
                SubHash(comm);
                break;
            case 3:
                SubSorting(comm);
                break;
        }

    }

    static void TopSearchItem(SqlConnection cnn, MPI.Intracommunicator comm, string searchTarget)
    {
        List<string> subElements;
        //string searchTarget = "55A8E43-A1C1-4320-B6F9-6A";
        string query = $"SELECT CarrierTrackingNumber from {tabel}";
        Stopwatch stopWatch = new Stopwatch();


        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(1, dest, 0);
        }


        List<string> elements = MakeRequest(cnn, query);
        cnn.Close();

        stopWatch.Start();

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

        stopWatch.Stop();

        printTime(stopWatch, "Sending data ended with time: ");

        stopWatch.Start();

        for (int i = 1; i < comm.Size; i++)
        {
            Console.WriteLine(comm.Receive<string>(i, 1));
        }

        printTime(stopWatch, "Mission completed with time: ");
    }


    static void SubSearchForItem(MPI.Intracommunicator comm)
    {
        while (true)
        {
            List<string> part;
            string searchTarget;
            string answer = "Not found";

            //Console.WriteLine("Waiting search target...");
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

            printTime(stopWatch, "Subtask ended with time: ");

            //Console.WriteLine("{0} process completed task! with size {1}", MPI.Environment.HostRank, part.Count);
            //zConsole.WriteLine("Last element: {0}", part.Last());
        }


    }
    static void TopHash(SqlConnection cnn, MPI.Intracommunicator comm)
    {
        List<string> subElements;
        List<string> result = new List<string>(2048);
        string searchTarget = "null";
        string query = $"SELECT PasswordHash from {tabelPasswords}";


        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(2, dest, 0);
        }

        List<string> elements = MakeRequest(cnn, query);
        cnn.Close();


        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

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

        stopWatch.Stop();

        printTime(stopWatch, "Sending data ended with time: ");

        stopWatch.Start();

        for (int i = 1; i < comm.Size; i++)
        {
            result.AddRange(comm.Receive<List<string>>(i, 1));
        }

        stopWatch.Stop();
        printTime(stopWatch, "Mission completed with time: ");
    }

    static void TopSorting(SqlConnection cnn, MPI.Intracommunicator comm)
    {
        List<int> subElements;
        List<int> result = new List<int>(2048);
        string searchTarget = "null";
        string query = $"SELECT SalesOrderID from {tabel}";


        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(3, dest, 0);
        }

        List<int> elements = MakeRequestInt(cnn, query);
        cnn.Close();


        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

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

        stopWatch.Stop();

        printTime(stopWatch, "Sending data ended with time: ");

        stopWatch.Start();

        for (int i = 1; i < comm.Size; i++)
        {
            result.AddRange(comm.Receive<List<int>>(i, 1));
        }

        result.Sort();

        //for (int i = 1; i < 10; i++){
        //    Console.WriteLine(result[i]);
        //}

        stopWatch.Stop();
        printTime(stopWatch, "Mission completed with time: ");
    }


    static void SubHash(MPI.Intracommunicator comm)
    {
        while (true)
        {
            List<string> part;
            string searchTarget;
            List<string> answer = new List<string>(100);

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

                answer.Add(Convert.ToString(item.GetHashCode()));

            }
            stopWatch.Stop();

            printTime(stopWatch, "Subtask ended with time: ");

            stopWatch.Start();

            comm.Send(answer, 0, 1);

            stopWatch.Stop();

            printTime(stopWatch, "Subtask sended with time: ");

            //Console.WriteLine("{0} process completed task! with size {1}", MPI.Environment.HostRank, part.Count);
            //zConsole.WriteLine("Last element: {0}", part.Last());
        }


    }

    static void SubSorting(MPI.Intracommunicator comm)
    {
        while (true)
        {
            List<int> part;
            string searchTarget;
            List<string> answer = new List<string>(100);

            searchTarget = comm.Receive<string>(0, 0);

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            if (searchTarget == "MPI.Kill")
            {
                break;
            }

            part = comm.Receive<List<int>>(0, 0);

            part.Sort();

            stopWatch.Stop();

            printTime(stopWatch, "Subtask ended with time: ");

            stopWatch.Start();

            comm.Send(part, 0, 1);


            stopWatch.Stop();

            printTime(stopWatch, "Subtask sended with time: ");
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

        printTime(stopWatch, "Sequental ended with time: ");

    }

    static void SequentalHash()
    {
        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        string query = $"SELECT PasswordHash from {tabelPasswords}";

        SqlConnection cnn = SqlConnect();
        List<string> elements = MakeRequest(cnn, query);
        cnn.Close();

        List<string> answer = new List<string>(100);

        foreach (string item in elements)
        {
            answer.Add(Convert.ToString(item.GetHashCode()));
        }

        stopWatch.Stop();

        printTime(stopWatch, "Sequental ended with time: ");

    }

    static void SequentalSorting()
    {
        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        string query = $"SELECT SalesOrderID from {tabel}";

        SqlConnection cnn = SqlConnect();
        List<int> elements = MakeRequestInt(cnn, query);
        cnn.Close();

        elements.Sort();
        //BubbleSort(ref elements);

        stopWatch.Stop();

        printTime(stopWatch, "Sequental ended with time: ");

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

    static List<int> MakeRequestInt(SqlConnection cnn, string query)
    {
        SqlCommand command = new SqlCommand(query, cnn);
        SqlDataReader reader = command.ExecuteReader();

        List<int> elements = new List<int>(2048);
        while (reader.Read())
        {
            elements.Add((int)reader.GetValue(0));
        }

        return elements;
    }

    static void printTime(Stopwatch watch, String description)
    {
        TimeSpan ts = watch.Elapsed;
        Console.WriteLine(description + String.Format("Sequental: {0:00}:{1:00}:{2:00}.{3:00}",
        ts.Hours, ts.Minutes, ts.Seconds,
        ts.Milliseconds / 10));
    }

    static void BubbleSort(ref List<int> a)
    {
        int temp;

        foreach (int aa in a)

            for (int p = 0; p <= a.Count - 2; p++)
            {
                for (int i = 0; i <= a.Count - 2; i++)
                {
                    if (a[i] > a[i + 1])
                    {
                        temp = a[i + 1];
                        a[i + 1] = a[i];
                        a[i] = temp;
                    }
                }
            }

    }
}