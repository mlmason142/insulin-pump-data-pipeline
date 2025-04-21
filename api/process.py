import os
from tandemsource import TandemSourceApi
from datetime import date, timedelta
import json
from dotenv import load_dotenv, dotenv_values



def api_auth(pumper):
    load_dotenv()
   
    email = os.getenv(f'{pumper}_EMAIL')
    password = os.getenv(f'{pumper}_PASSWORD')
    return TandemSourceApi(email,password)
def date_range_chunks(start_date, end_date, chunk_size):
    """
    Iterates through a date range in chunks.

    Args:
        start_date (date): The start date of the range.
        end_date (date): The end date of the range.
        chunk_size (int): The number of days in each chunk.

    Yields:
        tuple: A tuple containing the start and end date of each chunk.
    """
    current_date = start_date
    while current_date <= end_date:
        chunk_end_date = min(current_date + timedelta(days=chunk_size -1), end_date)
        yield current_date, chunk_end_date
        current_date = chunk_end_date + timedelta(days=1)

def get_hist_events(startDate=None, endDate=None, numDays=None):
    if endDate == None: 
        endDate = date.today()
        print(endDate)
    if startDate == None:
        startDate = date(2024, 9, 28)

    if numDays == None: 
        numDays = 5
    pumpers = ['P1', 'P2']
    for pumper in pumpers:
        email = os.getenv(f'{pumper}_EMAIL')
        print('email', email)
        password = os.getenv(f'{pumper}_PASSWORD')
        pump_id = os.getenv(f'{pumper}_PUMP_ID')
        for chunk_start, chunk_end in date_range_chunks(startDate, endDate, numDays):
            print(f"Chunk start: {chunk_start}, Chunk end: {chunk_end}")
            data = TandemSourceApi(email,password).pump_events(pump_id, chunk_start, chunk_end)
            event_data = []
            unparsed_events = []
            for e in data:
                if isinstance (e, dict):
                    event_data.append(e)
            else:
                unparsed_events.append(e)
            load_dotenv()    

            path = os.getenv('PATH')
            event_path = f'{path}/hist/{chunk_start}-{chunk_end}/events/{pump_id}'
            if not os.path.isdir(event_path):
                os.makedirs(event_path)
                print("Directory created successfully!", event_path)
            else:
                print("Directory already exists!", event_path)   

            unparsed_path = f'{path}/hist/{chunk_start}-{chunk_end}/unparsed/{pump_id}' 
            if not os.path.isdir(unparsed_path):
                os.makedirs(unparsed_path)
                print("Directory created successfully!", unparsed_path)
            else:
                print("Directory already exists!", unparsed_path)  
    
            with open(f'{event_path}/events-{pump_id}-{chunk_start}-{chunk_end}.json', 'w') as f:
                json.dump(event_data, f) 
                #print(event_data) 

            with open(f'{unparsed_path}/unparsed-{pump_id}-{chunk_start}-{chunk_end}.txt', 'w') as f:
                f.write(str(unparsed_events))  
      


    return print('job done')



if __name__ == '__main__':
    get_hist_events()