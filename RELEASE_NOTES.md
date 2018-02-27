### 0.0.7
* Made changefeed processor handler function take range position as argument
* Added timestamp information to the changefeed processor tail tracker

### 0.0.6
* Added changefeed tail position tracking

### 0.0.5
* Fixed issue with partition workers exiting when reaching end of changefeed

### 0.0.4
* Changed `ChangeFeed` to `Changefeed` in type and module names for consistency

### 0.0.3
* Changed `PartitionPosition` to `RangePosition` (without `PartitionId`)
* Changed `ChangeFeedPosition` from `RangePosition list` to `RangePosition[]`
* Made `ChangeFeedPositionTracker` internal

### 0.0.2
* Assembly name fixup
* Dependencies minimum version numbers

### 0.0.1
* initial
