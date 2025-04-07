from api import * 
from api.tandemsource import TandemSourceApi
from datetime import date
from dotenv import load_dotenv
import pandas as ps
import os
import json

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
    data = TandemSourceApi().pump_events(pump_id, '2025-04-05', '2025-04-05')
    event_data = []
    unparsed_events = []
    for e in data:
        if isinstance (e, dict):
            event_data.append(e)
        else:
            unparsed_events.append(e)
    path = os.getenv('PATH') 

    event_path = f'{path}/2025-04-05/daily/events/{pump_id}'
    if not os.path.isdir(event_path):
      os.makedirs(event_path)
      print("Directory created successfully!")
    else:
      print("Directory already exists!")   

    unparsed_path = f'{path}/2025-04-05/daily/unparsed/{pump_id}' 
    if not os.path.isdir(unparsed_path):
      os.makedirs(unparsed_path)
      print("Directory created successfully!")
    else:
      print("Directory already exists!")  
    
    with open(f'{event_path}/events-{pump_id}-2025-04-05.json', 'w') as f:
        json.dump(event_data, f)  

    with open(f'{unparsed_path}/unparsed-{pump_id}-2025-04-05.txt', 'w') as f:
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