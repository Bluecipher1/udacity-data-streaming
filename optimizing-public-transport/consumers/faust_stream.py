"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table", partitions=1)
table = app.Table(
    "stations_transformed",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def process_topics(topics):
    async for topic in topics:
        line = None

        if topic.red is True:
            line = "red"
        elif topic.blue is True:
            line = "blue"
        elif topic.green is True:
            line = "green"
        else:
            logger.warning(f"no color set for station {topic}")
            
        table[topic.station_id] = TransformedStation(
            station_id=topic.station_id,
            station_name=topic.station_name,
            order=topic.order,
            line=line,
        )
        
if __name__ == "__main__":
    app.main()
