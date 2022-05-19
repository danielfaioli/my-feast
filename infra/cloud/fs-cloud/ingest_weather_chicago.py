# Databricks notebook source
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

from sodapy import Socrata
from tqdm import tqdm
from datetime import datetime , timedelta

import concurrent

CLIENT = Socrata(
    "data.cityofchicago.org",
    "89mr9UrZJHvtFcdGNRbfVZLIg",
    timeout=36000
)

EH_CONNECT = "Endpoint=sb://myfeaststream.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=jE0uH39EZE4jIHDHVFvyrInUs7AunJrXxyl4lWAj+tY=;EntityPath=chicago_weather_daily"

class EventsProducer:
    def __init__(self, messages):
        self.messages = messages

    async def run(self):
        # Create a producer client to send messages to the event hub.
        # Specify a connection string to your event hubs namespace and
        # the event hub name.
        producer = EventHubProducerClient.from_connection_string(conn_str=EH_CONNECT, eventhub_name="chicago_weather_daily")
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()
            
            for mes in self.messages:
                event_data_batch.add(EventData(str(mes)))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)

sdate = datetime.strptime("2022-04-01", "%Y-%m-%d")
edate = datetime.strptime("2022-05-15", "%Y-%m-%d")

days = [sdate+timedelta(days=x) for x in range((edate-sdate).days)]

for day in days:
    DATA_GENERATOR = CLIENT.get_all(dataset_identifier="k7hf-8y75", where=f"measurement_timestamp >= '{day.strftime('%Y-%m-%d')}T00:00:00.000' \
    AND measurement_timestamp < '{(day+timedelta(days=1)).strftime('%Y-%m-%d')}T00:00:00.000'")        
    
    messages = []

    def helper(event):
        return event

    with concurrent.futures.ProcessPoolExecutor(5) as executor:
        for item in tqdm(DATA_GENERATOR, desc=day.strftime('%Y-%m-%d')):
            messages.append(executor.submit(helper, item).result())        

#     producer = EventsProducer(messages = messages)
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(producer.run())

