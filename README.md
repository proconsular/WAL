A simple log-structure storage engine.

Use HTTP REST commands to get and set key-value pairs.

Example:
    POST http://localhost:4000/emperor BODY Augustus
    GET http://localhost:4000/emperor RESPONSE Augustus

Write to an append-only log segment. When a threshold of records is hit, a new segment is made with an monotonically increasing id number.
Reads require all segments to read in order and all records to be read in order so the latest value can be read.

Example:
    GET emperor
        VAL = ""
        segment-1.log
            emperor:Augustus
            emperor:Caesar
        VAL = Caesar
        segment-2.log (last)
            emperor:Caligula
        VAL = Caligula
    return VAL (Caligula)

In order to speed up reads (which are slow) segment compaction is used. A separate thread either periodically or triggered reads a section of the segments and compacts them into a single segment with only the latest values.