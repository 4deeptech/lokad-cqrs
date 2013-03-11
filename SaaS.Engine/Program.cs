using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Security;
using System.Threading;
using Lokad.Cqrs;
using Lokad.Cqrs.Evil;
using SaaS.Wires;
using ServiceStack.Text;

namespace SaaS.Engine
{
    class Program
    {
        static void Main()
        {
            using (var env = BuildEnvironment())
            using (var cts = new CancellationTokenSource())
            {
                env.ExecuteStartupTasks(cts.Token);
                using (var engine = env.BuildEngine(cts.Token))
                {
                    var task = engine.Start(cts.Token);
                    for (int i = 1; i < 100; i++)
                    {
                        env.SendToCommandRouter.Send(new CreateSecurityAggregate(new SecurityId(i)));
                    }

                    Console.WriteLine(@"Press enter to stop");
                    Console.ReadLine();
                    cts.Cancel();
                    if (!task.Wait(5000))
                    {
                        Console.WriteLine(@"Terminating");
                    }
                }
            }
        }


        static void ConfigureObserver()
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            var observer = new ConsoleObserver();
            SystemObserver.Swap(observer);
            Context.SwapForDebug(s => SystemObserver.Notify(s));
        }

        public static Container BuildEnvironment()
        {
            //JsConfig.DateHandler = JsonDateHandler.ISO8601;
            ConfigureObserver();
            var integrationPath = AzureSettingsProvider.GetStringOrThrow(Conventions.StorageConfigName);
            //var email = AzureSettingsProvider.GetStringOrThrow(Conventions.SmtpConfigName);
            

            //var core = new SmtpHandlerCore(email);
            var setup = new Setup
            {
                //Smtp = core,
                //FreeApiKey = freeApiKey,
                //WebClientUrl = clientUri,
                //HttpEndpoint = endPoint,
                //EncryptorTool = new EncryptorTool(systemKey)
            };

            if (integrationPath.StartsWith("file:"))
            {
                var path = integrationPath.Remove(0, 5);

                SystemObserver.Notify("Using store : {0}", path);

                var config = FileStorage.CreateConfig(path);
                setup.Streaming = config.CreateStreaming();
                setup.DocumentStoreFactory = config.CreateDocumentStore;
                setup.QueueReaderFactory = s => config.CreateInbox(s, DecayEvil.BuildExponentialDecay(500));
                setup.QueueWriterFactory = config.CreateQueueWriter;
                setup.AppendOnlyStoreFactory = config.CreateAppendOnlyStore;

                setup.ConfigureQueues(1, 1);

                return setup.Build();
            }
            if (integrationPath.StartsWith("Default") || integrationPath.Equals("UseDevelopmentStorage=true", StringComparison.InvariantCultureIgnoreCase))
            {
                var config = AzureStorage.CreateConfig(integrationPath);
                setup.Streaming = config.CreateStreaming();
                setup.DocumentStoreFactory = config.CreateDocumentStore;
                setup.QueueReaderFactory = s => config.CreateQueueReader(s);
                setup.QueueWriterFactory = config.CreateQueueWriter;
                setup.AppendOnlyStoreFactory = config.CreateAppendOnlyStore;
                setup.ConfigureQueues(4, 4);
                return setup.Build();
            }
            throw new InvalidOperationException("Unsupported environment");
        }
    }
}
