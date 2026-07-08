Thanks for the context, Revanth — makes sense now. The backend reconciliation is designed to run as a batch over the full account set, so there isn't a standard way to process a single account on demand today. Fastest path for testing is usually triggering the ad-hoc batch run in QA rather than waiting for overnight. Whether we can filter the job to a specific account_id, I'd need to check how the DEVEX exchange and consolidator steps are configured — let me look into it and get back to you. Can you share the account_id and how often you'd need this during a test cycle?
Hi team — documenting eCBR alerting for Recoveries (CT4018T-544) and need a quick assist 🙏

*PagerDuty:* Who owns it? Is the flow eCBR detects → eCBR PagerDuty → pages our tenant group? Anything we set up on our side?

*Alert coverage — live today or planned?*
• Pipeline timing (midnight / 8am / 5am / 12pm)
• Runtime >4hr and >2hr
• Dataset refresh >24hr
• Stale upstream data
• Success notifications

Quick call works too. Thanks!
