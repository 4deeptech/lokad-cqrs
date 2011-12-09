﻿#region (c) 2010-2011 Lokad - CQRS for Windows Azure - New BSD License 

// Copyright (c) Lokad 2010-2011, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence

#endregion

using System;
using Lokad.Cqrs.Build.Engine;
using Lokad.Cqrs.Core.Dispatch;
using Lokad.Cqrs.Core.Outbox;
using Lokad.Cqrs.Core;

namespace Lokad.Cqrs.Feature.MemoryPartition
{
    public sealed class MemoryPartitionModule : HideObjectMembersFromIntelliSense, IAdvancedDispatchBuilder
    {
        readonly string[] _memoryQueues;

        HandlerFactory _dispatcher;
        Func<Container, IEnvelopeQuarantine> _quarantineFactory;

        public MemoryPartitionModule(string[] memoryQueues)
        {
            _memoryQueues = memoryQueues;
            Quarantine(c => new MemoryQuarantine());
        }

        public void DispatcherIs(Func<Container, ISingleThreadMessageDispatcher> factory)
        {
            _dispatcher = container =>
            {
                var d = factory(container);
                d.Init();
                return (envelope => d.DispatchMessage(envelope));
            };
        }

        /// <summary>
        /// Defines dispatcher as lambda method that is resolved against the container
        /// </summary>
        /// <param name="factory">The factory.</param>
        public void DispatcherIsLambda(HandlerFactory factory)
        {
            _dispatcher = factory;
        }

        public void Quarantine(Func<Container, IEnvelopeQuarantine> factory)
        {
            _quarantineFactory = factory;
        }

        public void DispatchToRoute(Func<ImmutableEnvelope, string> route)
        {
            DispatcherIs(ctx => new DispatchMessagesToRoute(ctx.Resolve<QueueWriterRegistry>(), route, ctx.Resolve<IEnvelopeStreamer>()));
        }

        IEngineProcess BuildConsumingProcess(Container context)
        {
            var log = context.Resolve<ISystemObserver>();
            var dispatcher = _dispatcher(context);

            var account = context.Resolve<MemoryAccount>();
            var notifier = account.GetMemoryInbox(_memoryQueues);

            var quarantine = _quarantineFactory(context);
            var manager = context.Resolve<MessageDuplicationManager>();
            var streamer = context.Resolve<IEnvelopeStreamer>();
            var transport = new DispatcherProcess(log, dispatcher, notifier, quarantine, manager, streamer);
           
            return transport;
        }

        public void Configure(Container container)
        {
            if (null == _dispatcher)
            {
                throw new InvalidOperationException(@"No message dispatcher configured, please supply one.

You can use either 'DispatcherIsLambda' or reference Lokad.CQRS.Composite and 
use Command/Event dispatchers. If you are migrating from v2.0, that's what you 
should do.");
            }

            var process = BuildConsumingProcess(container);
            var setup = container.Resolve<EngineSetup>();
            setup.AddProcess(process);
        }
    }
}