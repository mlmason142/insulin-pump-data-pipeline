from api import * 
from api.tandemsource import TandemSourceApi
from datetime import date
from dotenv import load_dotenv
import pandas as ps
import os

import dagster as dg



@dg.asset 

def get_alert_alarm_data() -> dg.MaterializeResult:
    load_dotenv()
    pump_id = os.getenv('PUMP_ID')
    today = date.today()
    data = TandemSourceApi().pump_events(pump_id, today, today,('4,5'))
    df = ps.DataFrame(data)

    return dg.MaterializeResult(metadata={"preview":dg.MetadataValue.md(df.to_markdown(index=False)),})

hourly_alert_job = dg.define_asset_job(
    "hourly_alert", selection=["get_alert_alarm_data"]
)

hourly_schedule = dg.ScheduleDefinition(
    job=hourly_alert_job,
    cron_schedule="0 * * * *"
)

@dg.asset 

def fetch_all_events() -> dg.MaterializeResult:
    load_dotenv()
    pump_id = os.getenv('PUMP_ID')
    today = date.today()
    print(today)
    data = TandemSourceApi().pump_events(pump_id, today, today)
    event_data = []
    for e in data:
        if isinstance (e, dict):
            event_data.append(e)
    df = ps.DataFrame(event_data)

    return dg.MaterializeResult(metadata={"preview":dg.MetadataValue.md(df.to_markdown(index=False)),})

daily_job = dg.define_asset_job(
    "daily_job", selection=["fetch_all_events"]
)

daily_schedule = dg.ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 0 * * *"
)


defs = dg.Definitions(
    assets=[get_alert_alarm_data, fetch_all_events],
    jobs=[hourly_alert_job, daily_job],
    schedules=[hourly_schedule, daily_schedule],
)