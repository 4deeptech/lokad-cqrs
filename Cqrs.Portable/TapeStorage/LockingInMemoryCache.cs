using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Lokad.Cqrs.TapeStorage
{
    /// <summary>
    /// Simple in-memory thread-safe cache
    /// </summary>
    public sealed class LockingInMemoryCache
    {
        readonly ReaderWriterLockSlim _thread = new ReaderWriterLockSlim();
        ConcurrentDictionary<string, DataWithVersion[]> _cacheByKey = new ConcurrentDictionary<string, DataWithVersion[]>();
        DataWithKey[] _cacheFull = new DataWithKey[0];

        public void ReloadEverything(IEnumerable<StorageFrameDecoded> sfd)
        {
            _thread.EnterWriteLock();
            try
            {
                _cacheFull = new DataWithKey[0];

                // [abdullin]: known performance problem identified by Nicolas Mehlei
                // creating new immutable array on each line will kill performance
                // We need to at least do some batching here

                var cacheFullBuilder = new List<DataWithKey>();
                var streamPointerBuilder = new Dictionary<string, List<DataWithVersion>>();

                long storeVersion = 0;
                foreach (var record in sfd)
                {
                    storeVersion += 1;
                    cacheFullBuilder.Add(new DataWithKey(record.Name, record.Bytes, record.Stamp, storeVersion));
                    List<DataWithVersion> list;
                    if (!streamPointerBuilder.TryGetValue(record.Name, out list))
                    {
                        streamPointerBuilder.Add(record.Name, list = new List<DataWithVersion>());
                    }
                    list.Add(new DataWithVersion(record.Stamp, record.Bytes, storeVersion));
                }

                _cacheFull = cacheFullBuilder.ToArray();
                _cacheByKey = new ConcurrentDictionary<string, DataWithVersion[]>(streamPointerBuilder.Select(p => new KeyValuePair<string, DataWithVersion[]>(p.Key, p.Value.ToArray())));
                _storeVersion = storeVersion;
            }
            finally
            {
                _thread.ExitWriteLock();
            }
        }

        static T[] ImmutableAdd<T>(T[] source, T item)
        {
            var copy = new T[source.Length + 1];

            Array.Copy(source, copy, source.Length);
            copy[source.Length] = item;


            return copy;
        }

        long _storeVersion;

        public long StoreVersion { get { return _storeVersion; } }

        public delegate void OnCommit(long streamVersion, long storeVersion);

        public void ConcurrentAppend(string streamName, byte[] data, OnCommit commit, long expectedStreamVersion = -1)
        {
            _thread.EnterWriteLock();

            try
            {
                var list = _cacheByKey.GetOrAdd(streamName, s => new DataWithVersion[0]);
                if (expectedStreamVersion >= 0)
                {
                    if (list.Length != expectedStreamVersion)
                        throw new AppendOnlyStoreConcurrencyException(expectedStreamVersion, list.Length, streamName);
                }
                long newStreamVersion = list.Length + 1;
                long newStoreVersion = _storeVersion + 1;

                commit(expectedStreamVersion, newStoreVersion);

                // update in-memory cache only after real commit completed

                var record = new DataWithVersion(newStreamVersion, data, newStoreVersion);
                _cacheFull = ImmutableAdd(_cacheFull, new DataWithKey(streamName, data, newStreamVersion, newStoreVersion));
                _cacheByKey.AddOrUpdate(streamName, s => new[] { record }, (s, records) => ImmutableAdd(records, record));
                _storeVersion = newStoreVersion;
            }
            finally
            {
                _thread.ExitWriteLock();
            }

        }

        public IEnumerable<DataWithVersion> ReadRecords(string streamName, long afterVersion, int maxCount)
        {
            if (afterVersion < 0)
                throw new ArgumentOutOfRangeException("afterVersion", "Must be zero or greater.");

            if (maxCount <= 0)
                throw new ArgumentOutOfRangeException("maxCount", "Must be more than zero.");

            // no lock is needed.
            DataWithVersion[] list;
            var result = _cacheByKey.TryGetValue(streamName, out list) ? list : Enumerable.Empty<DataWithVersion>();

            return result.Where(version => version.StoreVersion > afterVersion).Take(maxCount);

        }

        public IEnumerable<DataWithKey> ReadRecords(long afterVersion, int maxCount)
        {
            // collection is immutable so we don't care about locks
            return _cacheFull.Where(key => key.StoreVersion > afterVersion).Take(maxCount);
        }

        public void Clear(Action onCommit)
        {
            _thread.EnterWriteLock();
            try
            {
                onCommit();
                _cacheFull = new DataWithKey[0];
                _cacheByKey.Clear();
                _storeVersion = 0;
            }
            finally
            {
                _thread.ExitWriteLock();
            }
        }
    }
}