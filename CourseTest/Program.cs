using Microsoft.Data.SqlClient;
using System.Collections;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

class PingPong
{

    //Основные такблицы, с которыми производится работа
    static string tabel = "Sales.OrderTracking";
    static string tabelPasswords = "Person.Password";



    
    static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            //Ветка кода для главного процесса
            if (comm.Rank == 0)
            {
                int operation = Menu();

                //Подключение к БД
                SqlConnection cnn = SqlConnect();

                //Выбор операции
                switch (operation)
                {
                    //Поиск эдемента с определённым ID
                    case 1:
                        string searchTarget = "55A8E43-A1C1-4320-BDF9-6A";
                        TopSearchItem(cnn, comm, searchTarget);
                        SequentalSearch(searchTarget);
                        break;
                    //Хэширование столбца с паролями
                    case 2:
                        TopHash(cnn, comm);
                        SequentalHash();
                        break;
                    //Сортировка столюца
                    case 3:
                        TopSorting(cnn, comm);
                        SequentalSorting();
                        break;
                }


                //Отправка команд завершения процессов
                for (int dest = 1; dest < comm.Size; ++dest)
                {
                    comm.Send("MPI.Kill", dest, 0);
                }


            }
            else
            {
                //Ветка кода для подпроцессов
                Parallel(comm);
            }
        });

    }

    //Функция для подпроцессов
    static void Parallel(MPI.Intracommunicator comm)
    {
        //Получаем команду - какую из операция выполнять
        int command = comm.Receive<int>(0, 0);

        switch (command)
        {
            //Поиск элемента в столбце
            case 1:
                SubSearchForItem(comm);
                break;
            //Хэширование столбца с паролями
            case 2:
                SubHash(comm);
                break;
            //Сортировка столбца
            case 3:
                SubSorting(comm);
                break;
        }

    }

    //Функция главного процесса для поиска элемента
    static void TopSearchItem(SqlConnection cnn, MPI.Intracommunicator comm, string searchTarget)
    {
        //List для отправки данных подпроцессам
        List<string> subElements;
        //List для получения исходных данных
        List<string> elements;
        //Формирование запроса для БД
        string query = $"SELECT CarrierTrackingNumber from {tabel}";
        //Таймер для отслеживания времени операций
        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        //Отправляем подпроцессам номер операции
        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(1, dest, 0);
        }

        //Делаем запрос к БД и отключаемся
        elements = MakeRequest(cnn, query);
        cnn.Close();

        //Вычисляем количество подпроцессов
        int mpiSize = comm.Size - 1;
        //Вычисляем сколько элементов будет отправлено каждому подпроцессу
        int portionSize = elements.Count / mpiSize;

        //Отправка данных
        for (int dest = 1; dest < comm.Size; ++dest)
        {
            //Отправляем команду начала работы
            comm.Send(searchTarget, dest, 0);

            //Если это не последний подпроцесс
            if (dest != (comm.Size - 1))
            {
                //Помещаем в List часть данных размером - portionSize; с индекса -  (dest - 1) * portionSize
                subElements = elements.GetRange((dest - 1) * portionSize, portionSize);
                //Отправка данных
                comm.Send(subElements, dest, 0);
            }
            //Если это последний подпроцесс
            else
            {
                //Отправляем все оставшиеся элементы в elements
                // elements.Count - ((dest - 1) * portionSize) -> отправить остаток List'а
                subElements = elements.GetRange((dest - 1) * portionSize, elements.Count - ((dest - 1) * portionSize));
                comm.Send(subElements, dest, 0);
            }

        }

        //получаем время, затраченное на отправку данных
        stopWatch.Stop();
        printTime(stopWatch, "Sending data ended with time: ");

        //Запускаем время на определение звтраченного на поиск время
        stopWatch.Start();

        //Получаем данные с каждого подпроцесса
        for (int i = 1; i < comm.Size; i++)
        {
            Console.WriteLine(comm.Receive<string>(i, 1));
        }


        printTime(stopWatch, "Mission completed with time: ");
    }

    static void TopHash(SqlConnection cnn, MPI.Intracommunicator comm)
    {
        //Список для отправки данных
        List<string> subElements;
        //Результирующий список с хэшами
        List<string> result = new List<string>(2048);
        //Команда для подпроцессов
        string command = "null";
        //Текст запроса
        string query = $"SELECT PasswordHash from {tabelPasswords}";

        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        //Команда операции
        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(2, dest, 0);
        }

        //Получаем исходный столбец
        List<string> elements = MakeRequest(cnn, query);
        cnn.Close();

        //Кол-во подпроцессов
        int mpiSize = comm.Size - 1;
        //Расмер данных на каждый подпроцесс
        int portionSize = elements.Count / mpiSize;

        //Отправка данных
        for (int dest = 1; dest < comm.Size; ++dest)
        {
            //Отправка команды начала работы
            comm.Send(command, dest, 0);

            //Если не последний подпроцесс
            if (dest != (comm.Size - 1))
            {
                subElements = elements.GetRange((dest - 1) * portionSize, portionSize);
                comm.Send(subElements, dest, 0);
            }
            //Если последдний подпроцесс
            else
            {
                //Отправляем все оставшиеся элементы в elements
                subElements = elements.GetRange((dest - 1) * portionSize, elements.Count - ((dest - 1) * portionSize));
                comm.Send(subElements, dest, 0);
            }

        }

        //Время отправки данных
        stopWatch.Stop();
        printTime(stopWatch, "Sending data ended with time: ");

        stopWatch.Start();
        //Сохраняем все хэши паролей
        for (int i = 1; i < comm.Size; i++)
        {
            result.AddRange(comm.Receive<List<string>>(i, 1));
        }

        //Вывод первых 15 хэшей
        for (int i = 1; i < 15; i++)
        {
            Console.WriteLine(result[i]);
        }

        stopWatch.Stop();
        printTime(stopWatch, "Mission completed with time: ");
    }

    //Функция сортировки главного процесса
    static void TopSorting(SqlConnection cnn, MPI.Intracommunicator comm)
    {
        //Список для отправки данных
        List<int> subElements = new List<int>(2048);
        //команда
        string command = "null";
        //запрос для БД
        string query = $"SELECT SalesOrderID from {tabel}";


        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();

        // Команда операции
        for (int dest = 1; dest < comm.Size; ++dest)
        {
            comm.Send(3, dest, 0);
        }

        //Получаем столбец с данными
        List<int> elements = MakeRequestInt(cnn, query);
        cnn.Close();

        //Масси для результата
        int[] result = new int[elements.Count + 20];

        //Кол-во подпроцессов
        int mpiSize = comm.Size - 1;
        int minElem = elements.Min();
        int maxElem = elements.Max();
        //Интервал чисел для сортировки для каждого подпроцесса
        int portionSize = (maxElem - minElem) / mpiSize;

        for (int dest = 1; dest < comm.Size; ++dest)
        {
            //Очищаем List для новых данных
            subElements.Clear();
            comm.Send(command, dest, 0);

            if (dest != (comm.Size - 1))
            {
                for (int j = 0; j < elements.Count; j++) 
                {
                    //Нижняя граница чисел
                    int minRange = (minElem + ((dest - 1) * portionSize));
                    //Высшая граница чисел
                    int maxRange = (minElem + (portionSize * dest));
                    //если входит в границы - помещаем в List
                    if ((elements[j] >= minRange) && (elements[j] < maxRange)) 
                    {
                        subElements.Add(elements[j]);
                    }
                }
                //Console.WriteLine(subElements[0]);
                comm.Send(subElements, dest, 0);
            }
            //Если последний подпроцесс
            else
            {
                for (int j = 0; j < elements.Count; j++)
                {
                    int minRange = (minElem + ((dest - 1) * portionSize));
                    //Помещаем оставшиеся элементы 
                    if (elements[j] >= minRange)
                    {
                        subElements.Add(elements[j]);
                    }
                }
                //Отправляем массив с числами
                comm.Send(subElements, dest, 0);
            }


        }

        stopWatch.Stop();

        printTime(stopWatch, "Sending data ended with time: ");

        stopWatch.Start();

        for (int i = 1; i < comm.Size; i++)
        {

            List<int> temp = new List<int>(2048);
            temp.Clear();
            temp.AddRange(comm.Receive<List<int>>(i, 1));
            Console.WriteLine(temp[0] + "---"  + temp.Count);
            Console.WriteLine((((temp[0] - minElem) / portionSize) * temp.Count) + temp.Count);

            for(int j = 0; j < temp.Count; j++) 
            {
                result[(((temp[0] - minElem) / portionSize) * temp.Count)  + j] = temp[j];
            }
            Console.WriteLine((temp[0] - minElem));
            //result.InsertRange(minElem - ()temp[0], temp);
        }

        Console.WriteLine(result[0]);
        Console.WriteLine(result.Length);

        for (int i = 1; i < 20; i++) 
        {
            Console.WriteLine(result[result.Length - i]);
        }

        stopWatch.Stop();
        printTime(stopWatch, "Mission completed with time: ");
    }


    //Функция подпроцесса для поиска элемента
    static void SubSearchForItem(MPI.Intracommunicator comm)
    {
        while (true)
        {
            //Список для получения исходных данных
            List<string> part;
            //Какое значение искать
            string searchTarget;
            //Переменная для результата
            string answer = "Not found";

            //Получаем команду
            searchTarget = comm.Receive<string>(0, 0);

            //Если команда остановки - прекращаем работу
            if (searchTarget == "MPI.Kill")
            {
                break;
            }

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            //Получаем данные
            part = comm.Receive<List<string>>(0, 0);

            //Ищем заданный элемент в списке
            foreach (string item in part)
            {
                if (item == searchTarget)
                {
                    //Если нашли - в переменную с результатом
                    answer = item;
                }
            }

            //Отправляем результат
            comm.Send(answer, 0, 1);

            //Время работы подпроцесса
            stopWatch.Stop();
            printTime(stopWatch, "Subtask ended with time: ");
        }


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

            var alg = SHA512.Create();
            foreach (string item in part)
            {
                alg.ComputeHash(Encoding.UTF8.GetBytes(item));
                answer.Add(BitConverter.ToString(alg.Hash));

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


        var alg = SHA512.Create();
        foreach (string item in elements)
        {
            alg.ComputeHash(Encoding.UTF8.GetBytes(item));
            answer.Add(BitConverter.ToString(alg.Hash));
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
        Console.WriteLine(description + String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
        ts.Hours, ts.Minutes, ts.Seconds,
        ts.Milliseconds / 10));
    }

    // Меню для выбора операции
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