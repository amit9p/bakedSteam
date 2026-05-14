
I checked this. It is not breaking the flow today because "event_timestamp" values are coming in a timestamp-compatible string format, and Spark is still able to use that field in the current logic when reading with the code-defined schema. That is why the "orderBy(event_timestamp)" path is working.

But it is still a schema mismatch between catalog and code, so we should align it to avoid future issues.
