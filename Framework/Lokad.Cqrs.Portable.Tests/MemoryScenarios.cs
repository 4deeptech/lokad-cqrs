﻿#region (c) 2010-2011 Lokad - CQRS for Windows Azure - New BSD License 

// Copyright (c) Lokad 2010-2011, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence

#endregion

using System;
using System.Linq;
using Lokad.Cqrs.Build.Engine;
using Lokad.Cqrs.Core.Dispatch.Events;
using Lokad.Cqrs.Scenarios;
using Lokad.Cqrs.Tests;
using NUnit.Framework;

namespace Lokad.Cqrs
{
    public static class MemoryScenarios
    {
        // ReSharper disable InconsistentNaming

        
        [TestFixture]
        public sealed class MemoryQuarantine : When_sending_failing_messages
        {
            public MemoryQuarantine()
            {
                EnlistFixtureConfig(builder => builder.Memory(x =>
                    {
                        x.AddMemoryProcess("in");
                        x.AddMemorySender("in");
                    }));
            }
        }
    }

}