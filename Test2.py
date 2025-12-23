
incidents_df = create_partially_filled_dataset(
    spark,
    IncidentService,
    data=[
        # 1–10 : FRAUD + IN_PROGRESS  -> TRUE
        *[
            {
                IncidentService.account_id: str(i),
                IncidentService.incident_type: constants.IncidentType.FRAUD.value,
                IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value,
            }
            for i in range(1, 11)
        ],

        # 11–20 : FRAUD + RESOLVED -> FALSE
        *[
            {
                IncidentService.account_id: str(i),
                IncidentService.incident_type: constants.IncidentType.FRAUD.value,
                IncidentService.incident_status: constants.IncidentStatus.RESOLVED.value,
            }
            for i in range(11, 21)
        ],

        # 21–30 : NON-FRAUD -> FALSE
        *[
            {
                IncidentService.account_id: str(i),
                IncidentService.incident_type: constants.IncidentType.DISPUTE.value,
                IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value,
            }
            for i in range(21, 31)
        ],

        # 31–39 : intentionally no rows → defaults to FALSE
    ]
)
