
incidents_df = create_partially_filled_dataset(
    spark,
    IncidentService,
    data=[
        # 1-7 -> FALSE
        {IncidentService.account_id: "1",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "2",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "3",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "4",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "5",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "6",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "7",  IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},

        # 8-9 -> TRUE
        {IncidentService.account_id: "8",  IncidentService.incident_type: constants.IncidentType.FRAUD.value, IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value},
        {IncidentService.account_id: "9",  IncidentService.incident_type: constants.IncidentType.FRAUD.value, IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value},

        # 10-14 -> FALSE
        {IncidentService.account_id: "10", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "11", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "12", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "13", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "14", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},

        # 15 -> TRUE
        {IncidentService.account_id: "15", IncidentService.incident_type: constants.IncidentType.FRAUD.value, IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value},

        # 16-36 -> FALSE
        {IncidentService.account_id: "16", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "17", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "18", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "19", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "20", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "21", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "22", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "23", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "24", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "25", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "26", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "27", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "28", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "29", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "30", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "31", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "32", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "33", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "34", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "35", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},
        {IncidentService.account_id: "36", IncidentService.incident_type: "OTHER", IncidentService.incident_status: "CLOSED"},

        # 37-39 -> TRUE
        {IncidentService.account_id: "37", IncidentService.incident_type: constants.IncidentType.FRAUD.value, IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value},
        {IncidentService.account_id: "38", IncidentService.incident_type: constants.IncidentType.FRAUD.value, IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value},
        {IncidentService.account_id: "39", IncidentService.incident_type: constants.IncidentType.FRAUD.value, IncidentService.incident_status: constants.IncidentStatus.IN_PROGRESS.value},
    ]
)


______



fraud_flag_df = fraud_claimed_flag(incidents_df) \
    .withColumnRenamed("is_fraud_claimed_on_account", "fraud_claimed_flag")

joined_df = joined_df.join(fraud_flag_df, on=BaseSegment.account_id.str, how="left")

joined_df = joined_df.fillna({"fraud_claimed_flag": False})

fraud_investigation_notification_true = (
    col("fraud_claimed_flag").cast("boolean") == True
)

