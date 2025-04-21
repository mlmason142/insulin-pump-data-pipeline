from api import * 
from api.tandemsource import TandemSourceApi
from datetime import date
from dotenv import load_dotenv, dotenv_values
import pandas as ps
import os
import json

import dagster as dg

load_dotenv()
#pumpers = os.getenv('pumpers')

@dg.asset 

def get_alert_alarm_data() -> dg.MaterializeResult:
    today = date.today()
    pumpers = ['P1', 'P2']
    for x in pumpers: 
        pump_id = os.getenv(f'{x}_PUMP_ID')
        email = os.getenv(f'{x}_EMAIL')
        password = os.getenv(f'{x}_PASSWORD')
        data = TandemSourceApi(email,password).pump_events(pump_id, today, today, ('4,5'))
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
    today = '2025-04-13'
    pumpers = ['P1', 'P2']
    for x in pumpers: 
        pump_id = os.getenv(f'{x}_PUMP_ID')
        email = os.getenv(f'{x}_EMAIL')
        password = os.getenv(f'{x}_PASSWORD')
        data = TandemSourceApi(email,password).pump_events(pump_id, today, today)
        event_data = []
        unparsed_events = []
        for e in data:
            if isinstance (e, dict):
                event_data.append(e)
            else:
                unparsed_events.append(e)
        path = os.getenv('PATH') 

        event_path = f'{path}/{today}/daily/events/{pump_id}'
        if not os.path.isdir(event_path):
            os.makedirs(event_path)
            print("Directory created successfully!")
        else:
            print("Directory already exists!")   

        unparsed_path = f'{path}/{today}/daily/unparsed/{pump_id}' 
        if not os.path.isdir(unparsed_path):
            os.makedirs(unparsed_path)
            print("Directory created successfully!")
        else:
            print("Directory already exists!")  
    
        with open(f'{event_path}/events-{pump_id}-{today}.json', 'w') as f:
            json.dump(event_data, f)  

        with open(f'{unparsed_path}/unparsed-{pump_id}-{today}.txt', 'w') as f:
            f.write(str(unparsed_events))  
      


    return print('job done')



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