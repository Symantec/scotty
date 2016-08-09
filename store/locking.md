# Locking in the scotty store package

To avoid contention as much as possible, scotty releases locks as quickly
as possible. For the same reason, Scotty avoids holding nested locks as much
as possible too.

## Lock Types

### timeSeriesType

Each individual time series has its own lock which client must obtain before
reading from or modifying the time series.

### timeStampSeriesType

List of timestamps for a particular group of time series. Each of these also
has its own lock.

## pageQueueType

The page queue is responsible for moving pages among time series and timestamp
series. The page queue also has its own lock, but it also must obtain locks
from time series and timestamp series as it moves pages amont them.
When using the page queue, clients must not hold locks on any time series or
timestamp series.

The consequence of this is that adding values to a time series cannot be an
atomic operation. Adding a value to a time series requires the following:

- Grab lock on time series
- try to add the value.
- Release lock on time series.
- If add failed
  * Grab lock on page queue
  * bestow a page to time series which implicitly grabs and releases its lock
  * Release lock on page queue
  * Try adding the same value again

Because scotty must store only unique values in a timestamp and because scotty
may have to release its lock on the time series to move pages around, only one
goroutine at a time can add a value to a particular time series. This is o.k as
scotty is engineered so that only one goroutine at a time will poll a particular
endpoint.

### timeSeriesCollectionType

All time series and timestamp series in an endpoint.
Each of these has its own lock for adding additional time series or timestamp
series. In addtion to its own lock, each time series collection has a special
*statusChange* lock. Clients must grab this special lock if it expects
to add any values to any of the time series or timestamp series within.
Clients must also grab this special lock if they expect to change the status
of the pages (active vs. insactive) the time series collection uses.

The status change lock serves two purposes:

1. Ensures only one goroutine at a time adds values to the time series in a
   time series collection.
2. Ensures that when marking a time series collection inactive, 
   eventually the status of all the pages used by it will become inactive.

### Store level

The store data structure contains all the endpoints. The store itself is
lockless. Adding / removing endpoints requires creating a brand new store.

## Preventing deadlock

To prevent deadlocks, we avoid nested locking whenever possible, but when
nested locking is needed, we lock in the following order:

1. time series collection status change lock
2. time series collection lock
3. page queue lock
4. time series lock
5. time stamp series lock

To acquire a particular lock, a goroutine must release any lock below it in the list. For example, to acquire the page queue lock, a goroutine must release
any time series locks and time stamp series locks it already has.
