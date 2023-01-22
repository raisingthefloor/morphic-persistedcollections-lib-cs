// Copyright 2022-2023 Raising the Floor - US, Inc.
//
// Licensed under the New BSD license. You may not use this file except in
// compliance with this License.
//
// You may obtain a copy of the License at
// https://github.com/raisingthefloor/morphic-persistedcollections-lib-cs/blob/main/LICENSE.txt
//
// The R&D leading to these results received funding from the:
// * Rehabilitation Services Administration, US Dept. of Education under
//   grant H421A150006 (APCP)
// * National Institute on Disability, Independent Living, and
//   Rehabilitation Research (NIDILRR)
// * Administration for Independent Living & Dept. of Education under grants
//   H133E080022 (RERC-IT) and H133E130028/90RE5003-01-00 (UIITA-RERC)
// * European Union's Seventh Framework Programme (FP7/2007-2013) grant
//   agreement nos. 289016 (Cloud4all) and 610510 (Prosperity4All)
// * William and Flora Hewlett Foundation
// * Ontario Ministry of Research and Innovation
// * Canadian Foundation for Innovation
// * Adobe Foundation
// * Consumer Electronics Association Foundation

using Morphic.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using static System.Collections.Specialized.BitVector32;

namespace Morphic.Collections.Persisted;

// types of actions (as represented in the in-memory and on-disk records
internal struct QueueAction
{
    public const string Enqueue = "enqueue";
    public const string Dequeue = "dequeue";
}

// log entries, as recorded in our persisted storage (on-disk)
internal struct OnDiskLogEntry<T> where T : struct
{
    [JsonPropertyName("action")]
    public string action { get; set; } // QueueAction
    //
    [JsonPropertyName("timestamp")]
    public string? timestamp { get; set; } // original timestamp, in utc (i.e. in UTC string format); note that this is the corollary of Record<T>.OriginalTimestamp, not of "LoadedTimestamp"
    //
    [JsonPropertyName("data")]
    public T? data { get; set; } // the data itself

    public OnDiskLogEntry(string action, string? timestamp = null, T? data = null)
    {
        this.action = action;
        this.timestamp = timestamp;
        this.data = data;
    }
}

// log entries, as loaded into or otherwise recorded in memory
internal class InMemoryLogEntry<T> where T : struct
{
    public string Action; // QueueAction
    public long MillisecondsSinceStartup; // timestamp when the log entry was loaded into memory, in milliseconds since startup (startup being a per-session fixed point in time--queue initialization time or application/system startup--and NOT based on the calendar clock as we don't want clock re-syncing to mess up our record timestamps)
    public QueueItem<T>? QueueItem;

    public InMemoryLogEntry(string action, long millisecondsSinceStartup, QueueItem<T>? queueItem)
    {
        this.Action = action;
        this.MillisecondsSinceStartup = millisecondsSinceStartup;
        this.QueueItem = queueItem;
    }
}

// NOTE: a QueueItem represents the actual data in the queue; the OnDiskLogEntry and InMemoryLogEntry structs/classes are used to log each queue action so we can save the queue to disk in an append-only fashion (and rebuild the queue from disk at a later time)
internal class QueueItem<T> where T : struct
{
    public readonly DateTimeOffset OriginalTimestamp; // original timestamp of record, in UTC
    public readonly T Data;

    private readonly List<InMemoryLogEntry<T>> _inMemoryLogEntries = new();
    private object _inMemoryLogEntriesLock = new();

    public QueueItem(T data, DateTimeOffset originalTimestamp)
    {
        this.Data = data;
        this.OriginalTimestamp = originalTimestamp;
    }

    // NOTE: as this queued item is added to our in-memory queue and THEN to our log, we provide a function to THEN add a reference to the log entry to this class
    //       [when the record is dequeued, we also use this function to reference the dequeue entry]
    public void AddInMemoryLogEntry(InMemoryLogEntry<T> entry)
    {
        lock (_inMemoryLogEntriesLock)
        {
            _inMemoryLogEntries.Add(entry);
        }
    }

    // NOTE: this function intentionally returns a cloned list of our in-memory log entries (with pointers to the original entries)
    internal List<InMemoryLogEntry<T>> GetInMemoryLogEntries()
    {
        List<InMemoryLogEntry<T>> result = new();
        lock (_inMemoryLogEntriesLock)
        {
            foreach (var entry in _inMemoryLogEntries)
            {
                result.Add(entry);
            }
        }

        return result;
    }
}

public class AppendOnlyFilePersistedQueue<T> where T : struct
{
    // NOTE: INCLUDE_QUEUE_ITEM_IN_LOG_EVENT is set when debugging so that we can match up dequeued events to enqueued events in the log.  We disable this normally as it's creating duplicate copies of STRUCTS which won't match up to each other in reference comparison...and are otherwise wasteful and not useful
#if DEBUG
    const bool INCLUDE_QUEUE_ITEM_IN_LOG_EVENT = true;
#else
    const bool INCLUDE_QUEUE_ITEM_IN_LOG_EVENT = false;
#endif

    // NOTE: we set a maximum size that the log file is allowed to grow to; after this size, we will alert the watcher to the error (and we'll stop writing out new entries to the log file...basically freezing the on-disk representation of the queue in time from a persisted perspective until it the queue is emptied)
    const long MAX_ALLOWED_PERSISTED_LOG_FILE_SIZE = 100 * 1024 * 1024;

    // NOTE: _queue contains the actual queue entries
    private List<QueueItem<T>> _queue = new();
    //
    // NOTE: _inMemoryLogEntries holds a list of all queue actions (which is what is persisted out to disk and is what we can load from disk to rebuild a queue)
    private List<InMemoryLogEntry<T>> _inMemoryLogEntries = new();
    //
    private readonly List<InMemoryLogEntry<T>> _newInMemoryLogEntriesToPersist = new();
    //
    // NOTE: we have one lock which locks the queue and the log entries lists together (to keep things simple); we tried using multiple locks for extra efficiency, but the code became quite complicated and there was an unreasonable risk of deadlock
    private object _queueAndLogEntriesLock = new();
    //internal object _queueLock = new();
    //object _inMemoryLogEntriesLock = new();
    //private object _newInMemoryLogEntriesToPersistLock = new();

    // NOTE: the stopwatch provided a millisecond counter from the moment our queue is created
    private Stopwatch _stopwatch;

    System.Threading.Timer _updatePersistedLogTimer;
    bool _updatePersistedLogTimerIsPending = false;
    object _updatePersistedLogTimerLock = new();

    #region Persistance timer and trigger functions

    // by default, persist log entries after 10 seconds
    TimeSpan _timeUntilPersist = new(0, 0, 10);
    public TimeSpan TimeUntilPersist
    {
        get
        {
            return _timeUntilPersist;
        }
        set
        {
            _timeUntilPersist = value;

            // out of an abundance of caution, persist immediately whenever the TiemUntilPersist is updated (in case anything was waiting to be persisted from the in-memory log)
            this.TriggerUpdatePersistedLogTimer(TimeSpan.Zero, true);
        }
    }

    public async Task FlushToDiskAsync()
    {
        // persist immediately
        await Task.Run(() =>
        {
            // NOTE: this code is the only code allowed to call UpdatePersistedLog directly, as the caller is waiting synchronously for us to complete
            this.UpdatePersistedLog(null);
        });
    }

    void TriggerUpdatePersistedLogTimer(TimeSpan dueTime, bool updateEvenIfTimerIsAlreadyPending = false)
    {
        // trigger the _updatePersistedLogTimer (to persist our action's log entries out to disk as necessary)
        lock (_updatePersistedLogTimerLock)
        {
            // NOTE: technically it should always be safe to force-trigger; we only check to see if it's already running for efficiency reasons (to avoid unnecessary re-entrancy checks and retriggering)
            bool executeTrigger = false;
            // if the log timer is inactive, then we need to trigger it in all circumstances
            if (_updatePersistedLogTimerIsPending == false)
            {
                executeTrigger = true;
            } 
            else
            {
                // if the caller asked for an override, we should also always trigger the timer
                if (updateEvenIfTimerIsAlreadyPending == true)
                {
                    executeTrigger = true;
                }
            }

            if (executeTrigger == true)
            {
                _updatePersistedLogTimerIsPending = true;
                _updatePersistedLogTimer.Change(dueTime, Timeout.InfiniteTimeSpan);
            }
        }
    }

    #endregion Persistance timer and trigger functions

    public class ItemEnqueuedEventArgs : EventArgs
    {
        public T Value;

        internal ItemEnqueuedEventArgs(T value)
        {
            this.Value = value;
        }
    }
    public event EventHandler<ItemEnqueuedEventArgs>? _itemEnqueued = null;
    public event EventHandler<ItemEnqueuedEventArgs>? ItemEnqueued
    {
        add
        {
            lock (_itemEnqueuedLock)
            {
                _itemEnqueued += value;
            }
        }
        remove
        {
            lock (_itemEnqueuedLock)
            {
                _itemEnqueued -= value;
            }
        }
    }
    private object _itemEnqueuedLock = new();

    public class ExceptionThrownEventArgs : EventArgs
    {
        public Exception Exception;

        internal ExceptionThrownEventArgs(Exception exception)
        {
            this.Exception = exception;
        }
    }
    public event EventHandler<ExceptionThrownEventArgs> ExceptionThrownWhileAppendingToFile;

    private string? _pathToPersistedLogFile = null;

    // NOTE: this constructor is used if we're creating a new queue
    public AppendOnlyFilePersistedQueue(string? pathToPersistedLogFile = null)
    {
        _pathToPersistedLogFile = pathToPersistedLogFile;

        _updatePersistedLogTimer = new Timer(this.UpdatePersistedLog);

        // start our timestamp stopwatch
        _stopwatch = Stopwatch.StartNew();
    }

    // NOTE: this constructor is used (privately) if we're restoring a queue from a file
    private AppendOnlyFilePersistedQueue(List<QueueItem<T>> queue, List<InMemoryLogEntry<T>> inMemoryLogEntries, string? pathToPersistedLogFile = null) 
        : this(pathToPersistedLogFile)
    {
        // NOTE: we already initialize _pathToPersistedLogFile and _stopwatch by calling the public constructor (above)

        // additionally, set up our queue and in-memory log entries based on the passed-in value (i.e. the queue reconstructed from the persisted log file and the corresponding log entries needed to persist it back to disk)
        _queue = queue;
        _inMemoryLogEntries = inMemoryLogEntries;

        // trigger the update persisted log timer immediately; this will help clear out (and in the future, clean up) our persisted log file (if a path to a pathToPersistedLogFile is set)
        this.TriggerUpdatePersistedLogTimer(TimeSpan.Zero, true);
    }

    //

    public record FromFileError : MorphicAssociatedValueEnum<FromFileError.Values>
    {
        // enum members
        public enum Values
        {
            ExceptionError/*(Exception ex)*/,
            FileContentsAreInvalid,
        }

        // functions to create member instances
        public static FromFileError ExceptionError(Exception ex) => new(Values.ExceptionError) { Exception = ex };
        public static FromFileError FileContentsAreInvalid => new(Values.FileContentsAreInvalid);

        // associated values
        public Exception? Exception { get; private set; }

        // verbatim required constructor implementation for MorphicAssociatedValueEnums
        private FromFileError(Values value) : base(value) { }
    }
    //
    public static async Task<MorphicResult<AppendOnlyFilePersistedQueue<T>, FromFileError>> FromFileAsync(string path, bool appendNewQueueActionsAfterLoad = true)
    {
        // STEP 1: read in the file contents

        byte[] allBytes;
        try
        {
            allBytes = await System.IO.File.ReadAllBytesAsync(path);
        }
        catch (Exception ex)
        {
            return MorphicResult.ErrorResult(FromFileError.ExceptionError(ex));
        }

        // STEP 2: break up the file into individual on-disk log entries

        List<OnDiskLogEntry<T>> onDiskLogEntries = new();

        // set up a UTF8 decoder that will throw an exception if the contents don't match UTF8 encoding
        var utf8Encoding = new UTF8Encoding(false, true);

        // separate the file contents along boundaries of [0xC0, 0x80]; this is an invalid UTF8 sequence (which would represent the null byte if alternates to 0x00 were permitted) and
        // we use this sequence to separate records in the append-only file contents
        // NOTE: every entry will be followed by the separator, even if that entry is the last entry in the file
        var currentStartIndex = 0;
        while (currentStartIndex < allBytes.Length)
        {
            bool foundSeparator = false;

            var index = currentStartIndex;
            while (index < allBytes.Length)
            {
                // look for [0xC0, 0x80] sequence
                if ((allBytes[index] == 0xC0) && (allBytes.Length > index + 1))
                {
                    if (allBytes[index + 1] == 0x80)
                    {
                        // we found our sequence; break out now
                        foundSeparator = true;
                        break;
                    }
                }
                index += 1;
            }

            // NOTE: every entry must have a separator after it; if we did not find a separator, then abort with an error
            if (foundSeparator == false)
            {
                return MorphicResult.ErrorResult(FromFileError.FileContentsAreInvalid);
            }

            // capture the log entry (as represented in bytes)
            byte[] onDiskLogEntryAsBytes = new byte[index - currentStartIndex];
            Array.Copy(allBytes, currentStartIndex, onDiskLogEntryAsBytes, 0, onDiskLogEntryAsBytes.Length);

            // convert to a string using utf8 encoding
            string onDiskLogEntryAsString;
            try
            {
                onDiskLogEntryAsString = utf8Encoding.GetString(onDiskLogEntryAsBytes);
            }
            catch (DecoderFallbackException)
            {
                // invalid utf8 content
                return MorphicResult.ErrorResult(FromFileError.FileContentsAreInvalid);
            }
            catch (Exception ex)
            {
                // while we aren't aware of any other exceptions that might be thrown, we'll return any exception out of an abundance of caution
                return MorphicResult.ErrorResult(FromFileError.ExceptionError(ex));
            }

            // convert to an OnDiskEntry<T> instance using json decoding, and then add it to our collection
            OnDiskLogEntry<T> onDiskLogEntry;
            try
            {
                onDiskLogEntry = System.Text.Json.JsonSerializer.Deserialize<OnDiskLogEntry<T>>(onDiskLogEntryAsString, new System.Text.Json.JsonSerializerOptions() { });
            }
            catch (System.Text.Json.JsonException)
            {
                // invalid JSON
                return MorphicResult.ErrorResult(FromFileError.FileContentsAreInvalid);
            }
            catch (Exception ex)
            {
                // while we aren't aware of any other exceptions that might be thrown, we'll return any exception out of an abundance of caution
                return MorphicResult.ErrorResult(FromFileError.ExceptionError(ex));
            }
            onDiskLogEntries.Add(onDiskLogEntry);

            // advance the current start index to the next entry (or to the end of the stream)
            currentStartIndex = index + 2;
        }

        // STEP 3: convert the on-disk log entries into in-memory log entries (and a working queue)

        List<QueueItem<T>> queue = new();
        //
        List<InMemoryLogEntry<T>> inMemoryLogEntries = new();
        foreach (var onDiskLogEntry in onDiskLogEntries)
        {
            // convert the on-disk log entry into its corresponding in-memory representation
            var convertToInMemoryLogEntryResult = ConvertOnDiskLogEntryToInMemoryLogEntry(onDiskLogEntry, 0 /* millisecondsSinceStartup */);
            if (convertToInMemoryLogEntryResult.IsError == true)
            {
                return MorphicResult.ErrorResult(FromFileError.FileContentsAreInvalid);
            }
            var inMemoryLogEntry = convertToInMemoryLogEntryResult.Value!;

            // add the in-memory log entry to our list
            inMemoryLogEntries.Add(inMemoryLogEntry);

            // process the log entry to build our queue
            switch (inMemoryLogEntry.Action)
            {
                case QueueAction.Enqueue:
                    {
                        // NOTE: queueItem will always be non-null at this point in code (as we reject any Enqueue log entries without the necessary information)
                        queue.Add(inMemoryLogEntry.QueueItem!);
                    }
                    break;
                case QueueAction.Dequeue:
                    {
                        if (queue.Count > 0)
                        {
                            queue.RemoveAt(0);
                        }
                        else
                        {
                            // this file contains dequeue actions but there is nothing to dequeue; this file is therefore, by definition, corrupt/invalid
                            return MorphicResult.ErrorResult(FromFileError.FileContentsAreInvalid);
                        }
                    }
                    break;
                default:
                    throw new Exception("invalid code path");
            }
        }

        // STEP 4: create a new instance of our class, with a fully-constructed in-memory log and queue

        // NOTE: if appendNewQueueActionsAfterLoad was set to true, we pass in the path which we were provided (therefore the log will be appended to automatically as new entries are added to or removed from the queue)
        var result = new AppendOnlyFilePersistedQueue<T>(queue, inMemoryLogEntries, appendNewQueueActionsAfterLoad ? path : null);

        // STEP 5: return the new queue instance to our caller

        return MorphicResult.OkResult(result);
    }

    //

    public record ToFileError : MorphicAssociatedValueEnum<ToFileError.Values>
    {
        // enum members
        public enum Values
        {
            ExceptionError/*(Exception ex)*/,
            InMemoryLogEntryIsInvalid,
        }

        // functions to create member instances
        public static ToFileError ExceptionError(Exception ex) => new(Values.ExceptionError) { Exception = ex };
        public static ToFileError InMemoryLogEntryIsInvalid => new(Values.InMemoryLogEntryIsInvalid);

        // associated values
        public Exception? Exception { get; private set; }

        // verbatim required constructor implementation for MorphicAssociatedValueEnums
        private ToFileError(Values value) : base(value) { }
    }
    private async Task<MorphicResult<MorphicUnit, ToFileError>> CopyToFileAsync(string path)
    {
        byte[] fileContents;

        // convert our in-memory log entries into their corresponding on-disk log entries
        List<OnDiskLogEntry<T>> onDiskLogEntries = new();
        lock (_queueAndLogEntriesLock)
        {
            foreach (var inMemoryLogEntry in _inMemoryLogEntries)
            {
                var convertToOnDiskLogEntryResult = AppendOnlyFilePersistedQueue<T>.ConvertInMemoryLogEntryToOnDiskLogEntry(inMemoryLogEntry);
                if (convertToOnDiskLogEntryResult.IsError == true)
                {
                    return MorphicResult.ErrorResult(ToFileError.InMemoryLogEntryIsInvalid);
                }
                var onDiskLogEntry = convertToOnDiskLogEntryResult.Value!;

                onDiskLogEntries.Add(onDiskLogEntry);
            }
        }

        // convert our list of on-disk log entries into a serialized representation to write to disk
        fileContents = AppendOnlyFilePersistedQueue<T>.ConvertOnDiskLogEntriesToByteArray(onDiskLogEntries);

        // write out our serialized log entries to disk
        try
        {
            await System.IO.File.WriteAllBytesAsync(path, fileContents);
        }
        catch (Exception ex)
        {
            return MorphicResult.ErrorResult(ToFileError.ExceptionError(ex));
        }

        return MorphicResult.OkResult();
    }

    //

    private static byte[] ConvertOnDiskLogEntryToByteArray(OnDiskLogEntry<T> onDiskLogEntry)
    {
        List<OnDiskLogEntry<T>> onDiskLogEntries = new();
        onDiskLogEntries.Add(onDiskLogEntry);
        return AppendOnlyFilePersistedQueue<T>.ConvertOnDiskLogEntriesToByteArray(onDiskLogEntries);
    }

    private static byte[] ConvertOnDiskLogEntriesToByteArray(List<OnDiskLogEntry<T>> onDiskLogEntries)
    {
        List<byte> result = new();

        foreach (var onDiskLogEntry in onDiskLogEntries)
        {
            // convert our on-disk log entry to json
            var onDiskLogEntryAsJson = System.Text.Json.JsonSerializer.Serialize<OnDiskLogEntry<T>>(onDiskLogEntry, new System.Text.Json.JsonSerializerOptions() { });
            var onDiskLogEntryAsBytes = System.Text.Encoding.UTF8.GetBytes(onDiskLogEntryAsJson);

            // add the on-disk log entry in its UTF8-encoded byte format
            result.AddRange(onDiskLogEntryAsBytes);
            // add the record separator sequence (a sequence which is otherwise invalid UTF-8, as it would represent \0)
            result.AddRange(new byte[] { 0xC0, 0x80 });
        }

        return result.ToArray();
    }

    // NOTE: since we are converting an on-disk log entry into an in-memory log entry, we assume that our conversion time is zero milliseconds since startup
    private static MorphicResult<InMemoryLogEntry<T>, MorphicUnit> ConvertOnDiskLogEntryToInMemoryLogEntry(OnDiskLogEntry<T> onDiskLogEntry, long millisecondsSinceStartup = 0)
    {
        // action
        switch (onDiskLogEntry.action)
        {
            case QueueAction.Dequeue:
                break;
            case QueueAction.Enqueue:
                break;
            default:
                // unknown/invalid
                return MorphicResult.ErrorResult();
        }
        var action = onDiskLogEntry.action;

        // timestamp (required in some log entries)
        DateTimeOffset? timestamp = null;
        if (onDiskLogEntry.timestamp is not null)
        {
            DateTimeOffset timestampAsNonNullable;
            var success = DateTimeOffset.TryParseExact(onDiskLogEntry.timestamp, "O", CultureInfo.InvariantCulture, DateTimeStyles.None, out timestampAsNonNullable);
            if (success == false)
            {
                // invalid timestamp (usually due to invalid encoding)
                return MorphicResult.ErrorResult();
            }
            timestamp = timestampAsNonNullable;
        }
        //
        // mark this file as invalid if the timestamp field was required for this action
        if (timestamp is null)
        {
            switch (action)
            {
                case QueueAction.Enqueue:
                    // required field is missing
                    return MorphicResult.ErrorResult();
                case QueueAction.Dequeue:
                default:
                    // not required
                    break;
            }
        }

        // data (required in some log entries)
        //
        // mark this file as invalid if the data field was required for this action
        if (onDiskLogEntry.data is null)
        {
            switch (action)
            {
                case QueueAction.Enqueue:
                    // required field is missing
                    return MorphicResult.ErrorResult();
                case QueueAction.Dequeue:
                default:
                    // not required
                    break;
            }
        }

        QueueItem<T>? queueItem = null;
        // if timestamp and data were provided, create a queueItem for this data (and stamp it with its original timestamp)
        if (timestamp is not null && onDiskLogEntry.data is not null)
        {
            queueItem = new QueueItem<T>(onDiskLogEntry.data.Value, timestamp.Value);

            // NOTE: if INCLUDE_QUEUE_ITEM_IN_LOG_EVENT is set to false, we should remove any Data that comes along with a Dequeue record; see the Dequeue function for related notes
            if (onDiskLogEntry.action == QueueAction.Dequeue)
            {
                queueItem = null;
            }
        }

        // create an in-memory log entry representing this information
        var inMemoryLogEntry = new InMemoryLogEntry<T>(action, millisecondsSinceStartup, queueItem);

        return MorphicResult.OkResult(inMemoryLogEntry);
    }

    private static MorphicResult<OnDiskLogEntry<T>, MorphicUnit> ConvertInMemoryLogEntryToOnDiskLogEntry(InMemoryLogEntry<T> inMemoryLogEntry)
    {
        // action
        switch (inMemoryLogEntry.Action)
        {
            case QueueAction.Dequeue:
                break;
            case QueueAction.Enqueue:
                break;
            default:
                // unknown/invalid action
                return MorphicResult.ErrorResult();
        }
        var action = inMemoryLogEntry.Action;

        // NOTE: at runtime, we do not enforce fields for in-memory log entries, as we assume they are not missing data in required fields
        // at debug time, assert if a required field is missing
        //
        // QueueItem (required for some actions)
        if (inMemoryLogEntry.QueueItem is null)
        {
            switch (inMemoryLogEntry.Action)
            {
                case QueueAction.Enqueue:
                    // field is required
                    Debug.Assert(false, "Required field is null");
                    //return MorphicResult.ErrorResult();
                    break;
                case QueueAction.Dequeue:
                default:
                    // field is not required
                    break;
            }
        }

        string? timestampAsString = null;
        T? data = null;

        var queueItem = inMemoryLogEntry.QueueItem;
        if (queueItem is not null)
        {
            timestampAsString = queueItem.OriginalTimestamp.ToString("O");
            data = queueItem.Data;
        }

        var result = new OnDiskLogEntry<T>(action, timestampAsString, data);
        return MorphicResult.OkResult(result);
    }

    //

    public int Count
    {
        get
        {
            lock (_queueAndLogEntriesLock)
            {
                return _queue.Count;
            }
        }
    }

    public void Enqueue(T value)
    {
        // create a queue item to hold the provided value
        var queueItem = new QueueItem<T>(value, DateTimeOffset.UtcNow);

        // NOTE: edge cases where data about entries could be stored in different lists in the wrong order (i.e. queue, logs, etc.) are dangerous; therefore, we lock both the queue and the in-memory log entries at the
        //       same time to ensure that our queue item is enqueued and that its action is stored in the log at the same time (i.e. so that nothing gets inserted out-of-order)
        //
        var millisecondsSinceStartup = _stopwatch.ElapsedMilliseconds;
        var inMemoryEnqueueLogEntry = new InMemoryLogEntry<T>(QueueAction.Enqueue, millisecondsSinceStartup, queueItem);
        lock (_queueAndLogEntriesLock)
        {
            _inMemoryLogEntries.Add(inMemoryEnqueueLogEntry);
            // save our new in-memory log entry to the "new log entries to persist" queue; we must do this here because we need to preserve the order in which entries are persisted
            _newInMemoryLogEntriesToPersist.Add(inMemoryEnqueueLogEntry);
            //
            // add our in-memory enqueue log entry to our queue item
            queueItem.AddInMemoryLogEntry(inMemoryEnqueueLogEntry);

            // enqueue the item (with the provided value) into our queue
            // NOTE: we do this second, out of an abundance of caution, in case we edit this code in the future to remove the comprehensive lock.  If we do that, we have to make sure that
            //       the queue item is added AFTER the in-memory log entry lest entries could get added to the in-memory in the wrong order (i.e. if multiple threads were attempting Enqueues at the same time)
            _queue.Add(queueItem);
        }

        // if the update persisted log timer is not pending, trigger it after our "time until persist" timespan; this will make sure our new log entries get written out to disk
        this.TriggerUpdatePersistedLogTimer(_timeUntilPersist, false);

        // notify any watchers that we have queued an event
        Delegate[]? invocationList;
        lock (_itemEnqueuedLock)
        {
            invocationList = _itemEnqueued?.GetInvocationList();
        }
        if (invocationList is not null)
        {
            foreach (EventHandler<ItemEnqueuedEventArgs> element in invocationList!)
            {
                // NOTE: each invocation for this event will effectively be fired in parallel; it is the responsibility of the caller to provide thread safety (i.e. handle events sequentially)
                Task.Run(() => {
                    // NOTE: while we are providing the listener with the enqueued value, we are not providing the listener with our queue entry or related in-memory log entries; the listener will typically use the passed value for logging purposes and will .Peek() or .Dequeue() if they want to actually get or remove an element from the front of the queue
                    element.Invoke(this, new ItemEnqueuedEventArgs(value));
                });
            }
        }
    }

    //

    // NOTE: this function returns the oldest element if one exists; otherwise it returns an error
    public MorphicResult<T, MorphicUnit> Peek()
    {
        lock (_queueAndLogEntriesLock)
        {
            if (_queue.Count > 0)
            {
                var result = _queue[0].Data;
                return MorphicResult.OkResult(result);
            }
            else
            {
                return MorphicResult.ErrorResult();
            }
        }
    }

    //

    // NOTE: this function removes and returns the oldest element if one exists; otherwise it returns an error
    public MorphicResult<T, MorphicUnit> Dequeue()
    {
        // NOTE: for debugging purposes, we can include the queue item in our DEQUEUE log events.  But since dequeuing is only deleting data (not adding it),
        // it's not necessary in production code (and would just clutter on-disk files); it would also create incorrect references in the data which we read
        // back in (since the ENQUEUE and DEQUEUE actions would point to different copies of the same queue item).

        QueueItem<T> queueItem;
        var isLastItemInQueue = false;
        lock (_queueAndLogEntriesLock)
        {
            if (_queue.Count == 0)
            {
                return MorphicResult.ErrorResult();
            }

            queueItem = _queue[0];
            _queue.RemoveAt(0);

            // if we just removed the last item in the queue, capture that information (so we can trigger our persistance state machine immediately)
            // NOTE: the purpose of the delay before persisting data is mostly there for enqueueing events (as they often get dequeued before they need to be persisted--and therefore don't need to ever be persisted);
            //       if the queue has been emptied then it makes sense to delete the persisted (on-disk) log files immediate; ideally we'd also compact the log file if it started growing, but to do so we'd need to track
            //       which in-memory (and on-disk) log entries could be purged, which is beyond the scope of this implementation.
            if (_queue.Count == 0)
            {
                isLastItemInQueue = true;
            }
        }

        lock (_queueAndLogEntriesLock)
        {
            // create a "dequeue" in-memory log entry for this element
            var millisecondsSinceStartup = _stopwatch.ElapsedMilliseconds;
            var inMemoryDequeueLogEntry = new InMemoryLogEntry<T>(QueueAction.Dequeue, millisecondsSinceStartup, INCLUDE_QUEUE_ITEM_IN_LOG_EVENT ? queueItem : null);
            //
            _inMemoryLogEntries.Add(inMemoryDequeueLogEntry);
            // save our new in-memory log entry to the "new log entries to persist" queue; we must do this here because we need to preserve the order in which entries are persisted
            _newInMemoryLogEntriesToPersist.Add(inMemoryDequeueLogEntry);
            //
            // add our in-memory dequeue log entry to our queue item
            queueItem.AddInMemoryLogEntry(inMemoryDequeueLogEntry);
        }

        if (isLastItemInQueue == true)
        {
            // trigger the update persisted log timer (so it can delete the on-disk log if no further entries have been enqueued before it is called momentarily)
            this.TriggerUpdatePersistedLogTimer(TimeSpan.Zero, true);
        }
        else
        {
            // if the update persisted log timer is inactive, trigger it after our "time until persist" timespan; this will make sure our new log entries get written out to disk
            this.TriggerUpdatePersistedLogTimer(_timeUntilPersist, false);
        }

        // return the dequeued item
        return MorphicResult.OkResult(queueItem.Data);
    }

    private object _updatePersistedLogReentryLock = new();
    private void UpdatePersistedLog(object? state)
    {
        // if the user has not specified a pth to the persisted file, no-op (return without any action) here
        if (_pathToPersistedLogFile is null)
        {
            return;
        }

        // NOTE: we _must_ protect this function against re-entry (just in case the timer fires while we're still working, due to the user changing the persistance time interval etc.)
        lock (_updatePersistedLogReentryLock)
        {
            // set our _updatePersistedLogTimerIsActive (so that our callers know to trigger us when they add new log items)
            // NOTE: locking _updatePersistedLogTimerLock probably isn't necessary, but we're doing it for parallelism and to make future changes to the code simpler
            lock (_updatePersistedLogTimerLock)
            {
                _updatePersistedLogTimerIsPending = false;
            }

            // check to see if our queue is empty; if it is, we should clear out our in-memory logs and also delete any persisted log file
            lock (_queueAndLogEntriesLock)
            {
                var queueCount = _queue.Count;
                if (queueCount == 0)
                {
                    // if our queue is empty, clear our in-memory logs (and our cache of logs to persist to disk)
                    // NOTE: in this implementation, we don't track which in-memory log entries could be purged (both because we don't build that list when we LOAD a file and also because we don't mark log entries as purgeable whenever their related items they are dequeued); we may
                    //       consider adding this optimization in the future (to enable compaction, etc.) -- but if we do so, there are quite a few edge cases to consider (and we should put debug-time checks throughout the code to make sure that isPurgeable is set to true in all the expected places)

                    lock (_queueAndLogEntriesLock)
                    {
                        _inMemoryLogEntries.Clear();
                        _newInMemoryLogEntriesToPersist.Clear();
                    }

                    // if our queue is empty, and if our log is persisted to disk, then delete the persisted log file (if it exists)
                    if (_pathToPersistedLogFile is not null)
                    {
                        try
                        {
                            if (System.IO.File.Exists(_pathToPersistedLogFile!) == true)
                            {
                                System.IO.File.Delete(_pathToPersistedLogFile!);
                            }
                        }
                        catch (Exception ex)
                        {
                            // notify any watchers that we have experienced an exception
                            this.RaiseExceptionThrownWhileAppendingToFile(ex);

                            // in all circumstances: if we cannot persist new entries to the log (or in this case...if we cannot delete the fully-persisted log already having purged our in-memory representations), abort
                            return;
                        }
                    }

                    // if our queue is empty, deleting the log file is our complete job here; there is no reason to re-trigger this timer as there is nothing else to persist.  So simply return now (and this function will be called again as needed, when the user takes an action which requires recreating the queue)
                    return;
                }
            }

            // NOTE: if we reach here, our queue has entries (and our in-memory log entries to persist list may have entries to persist as well)

            FileStream? persistedLogFileStream = null;

            // NOTE: this loop will execute until all "new in memory log entries" are persisted
            while (true)
            {
                InMemoryLogEntry<T> inMemoryLogEntry;
                lock (_queueAndLogEntriesLock)
                {
                    if (_newInMemoryLogEntriesToPersist.Count == 0)
                    {
                        break;
                    }

                    inMemoryLogEntry = _newInMemoryLogEntriesToPersist[0];
                }

                // step 1: open the log file for append (if it's not already open)
                if (persistedLogFileStream is null)
                {
                    try
                    {
                        // sanity-check: make sure that the log file does not exceed our maximum-allowed size
                        var persistedFileInfo = new System.IO.FileInfo(_pathToPersistedLogFile);
                        if (persistedFileInfo.Exists == true && persistedFileInfo.Length > MAX_ALLOWED_PERSISTED_LOG_FILE_SIZE)
                        {
                            var exceptionMessage = "Log file exceeds the max allowed log file size";
                            Debug.Assert(false, exceptionMessage);

                            // notify any watchers that we have experienced an exception
                            this.RaiseExceptionThrownWhileAppendingToFile(new Exception(exceptionMessage));

                            // in all circumstances: if we cannot persist new entries to the log, abort
                            return;
                        }

                        // open up the persisted log file on disk (either by creating it, or by opening it to append to it)
                        persistedLogFileStream = new FileStream(_pathToPersistedLogFile!, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.None);
                    }
                    catch (Exception ex)
                    {
                        Debug.Assert(false, "Exception when trying to open persisted queue's log file: " + ex.ToString());

                        // notify any watchers that we have experienced an exception
                        this.RaiseExceptionThrownWhileAppendingToFile(ex);

                        // in all circumstances: if we cannot persist new entries to the log, abort
                        return;
                    }
                }

                // step 2: serialize the in-memory log entry to write out
                var convertToOnDiskLogEntryResult = AppendOnlyFilePersistedQueue<T>.ConvertInMemoryLogEntryToOnDiskLogEntry(inMemoryLogEntry);
                if (convertToOnDiskLogEntryResult.IsError == true)
                {
                    Debug.Assert(false, "Exception while trying to serialize a persisted queue entry, before writing it out to disk.");

                    // notify any watchers that we have experienced an exception
                    this.RaiseExceptionThrownWhileAppendingToFile(new Exception("Could not convert in-memory log entry to on-disk log entry"));

                    // in all circumstances: if we cannot persist new entries to the log, abort
                    return;
                }
                var onDiskLogEntry = convertToOnDiskLogEntryResult.Value!;

                // convert our list of on-disk log entries into a serialized representation to write to disk
                var inMemoryLogEntryAsBytes = AppendOnlyFilePersistedQueue<T>.ConvertOnDiskLogEntryToByteArray(onDiskLogEntry);

                // write out our serialized log entry to disk
                try
                {
                    persistedLogFileStream!.Write(inMemoryLogEntryAsBytes, 0, inMemoryLogEntryAsBytes.Length);
                }
                catch (Exception ex)
                {
                    // notify any watchers that we have experienced an exception
                    this.RaiseExceptionThrownWhileAppendingToFile(ex);

                    // in all circumstances: if we cannot persist new entries to the log, abort
                    return;
                }

                lock (_queueAndLogEntriesLock)
                {
                    // step 3: remove the in-memory log entry from the "new in-memory log entries to persist" list
                    _newInMemoryLogEntriesToPersist.RemoveAt(0);
                }
            }

            // if we have opened up the persisted file to append to it, close it now
            persistedLogFileStream?.Dispose();

            // finally, before we release our reentry lock, if there are any additional entries in the _newInMemoryLogEntriesToPersist list: re-call our function by re-triggering its timer (just in case a multi-threaded timing edge case doesn't get it called)
            int newInMemoryLogEntriesToPersistCount;
            lock (_queueAndLogEntriesLock)
            {
                newInMemoryLogEntriesToPersistCount = _newInMemoryLogEntriesToPersist.Count;
            }
            if (newInMemoryLogEntriesToPersistCount > 0)
            {
                // trigger the update persisted log timer immediately
                this.TriggerUpdatePersistedLogTimer(TimeSpan.Zero, true);
            }
        }
    }

    private void RaiseExceptionThrownWhileAppendingToFile(Exception exception)
    {
        // notify any watchers that we have queued an event
        Delegate[]? invocationList;
        invocationList = this.ExceptionThrownWhileAppendingToFile?.GetInvocationList();
        if (invocationList is not null)
        {
            foreach (EventHandler<ExceptionThrownEventArgs> element in invocationList!)
            {
                // NOTE: each invocation for this event will effectively be fired in parallel; it is the responsibility of the caller to provide thread safety (i.e. handle events sequentially)
                Task.Run(() => {
                    element.Invoke(this, new ExceptionThrownEventArgs(exception));
                });
            }
        }

    }

}