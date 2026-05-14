


Looks good to me overall. Current fix should work fine for values like "2023-10-01 00:00:00".

Only non-blocking note: if upstream format changes later to something like "2023-10-01T00:00:00Z", "2023-10-01T00:00:00.123Z", or "10/01/2023 00:00:00", we may need explicit "to_timestamp()" parsing. For current input, LGTM.
I checked this. It is not breaking the flow today because "event_timestamp" values are coming in a timestamp-compatible string format, and Spark is still able to use that field in the current logic when reading with the code-defined schema. That is why the "orderBy(event_timestamp)" path is working.

But it is still a schema mismatch between catalog and code, so we should align it to avoid future issues.
