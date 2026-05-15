
Hi team, I’m testing the "POST /internal-operations/platform-management-services/jobs" API for adhoc credit report runs.

For "tenant = CARDRECOVERIES", the call is working fine with the same request structure.
For "tenant = CARDSMALLBUSINESS", the request reaches the API successfully, but the server returns "HTTP 500" with only:

"{"error_message":"Your request could not be completed."}"

So this does not look like a curl/connectivity/auth issue. It looks more like a backend processing or tenant/config onboarding issue specific to "CARDSMALLBUSINESS".

Can someone please help check whether:

- "CARDSMALLBUSINESS" is fully supported/onboarded for this API
- the "platformRunLibraryName", "platformRunLibraryVersion", and "tenantConfigurationFileName" combination is valid for this tenant
- there is any server-side validation or config failure visible in logs for this request

I can share the payload and "Client-Correlation-Id" if needed.
