﻿#region (c) 2010-2011 Lokad CQRS - New BSD License 

// Copyright (c) Lokad SAS 2010-2011 (http://www.lokad.com)
// This code is released as Open Source under the terms of the New BSD Licence
// Homepage: http://lokad.github.com/lokad-cqrs/

#endregion

using System;
using System.Linq;
using System.Transactions;
using Lokad.Cqrs.Core.Envelope;

namespace Lokad.Cqrs.Core.Outbox
{
    sealed class DefaultMessageSender : IMessageSender
    {
        readonly IQueueWriter[] _queues;
        readonly Func<string> _idGenerator;
        readonly IEnvelopeStreamer _streamer;

        public DefaultMessageSender(IQueueWriter[] queues, Func<string> idGenerator, IEnvelopeStreamer streamer)
        {
            _queues = queues;
            _idGenerator = idGenerator;
            _streamer = streamer;

            if (queues.Length == 0)
                throw new InvalidOperationException("There should be at least one queue");
        }

        public void SendOne(object content)
        {
            InnerSendBatch(cb => { }, new[] {content});
        }

        public void SendOne(object content, Action<EnvelopeBuilder> configure)
        {
            InnerSendBatch(configure, new[] {content});
        }


        public void SendBatch(object[] content)
        {
            if (content.Length == 0)
                return;

            InnerSendBatch(cb => { }, content);
        }

        public void SendBatch(object[] content, Action<EnvelopeBuilder> builder)
        {
            InnerSendBatch(builder, content);
        }

        public void SendControl(Action<EnvelopeBuilder> builder)
        {
            InnerSendBatch(builder, new object[0]);
        }


        readonly Random _random = new Random();


        void InnerSendBatch(Action<EnvelopeBuilder> configure, object[] messageItems)
        {
            var id = _idGenerator();

            var builder = new EnvelopeBuilder(id);
            foreach (var item in messageItems)
            {
                builder.AddItem(item);
            }

            configure(builder);
            var envelope = builder.Build();

            var queue = GetOutboundQueue();
            var data = _streamer.SaveEnvelopeData(envelope);

            if (Transaction.Current == null)
            {
                queue.PutMessage(data);

                SystemObserver.Notify(new EnvelopeSent(queue.Name, envelope.EnvelopeId, false,
                    envelope.Items.Select(x => x.MappedType.Name).ToArray(), envelope.GetAllAttributes()));
            }
            else
            {
                var action = new CommitActionEnlistment(() =>
                    {
                        queue.PutMessage(data);
                        SystemObserver.Notify(new EnvelopeSent(queue.Name, envelope.EnvelopeId, true,
                            envelope.Items.Select(x => x.MappedType.Name).ToArray(), envelope.GetAllAttributes()));
                    });
                Transaction.Current.EnlistVolatile(action, EnlistmentOptions.None);
            }
        }

        IQueueWriter GetOutboundQueue()
        {
            if (_queues.Length == 1)
                return _queues[0];
            var random = _random.Next(_queues.Length);
            return _queues[random];
        }
    }
}