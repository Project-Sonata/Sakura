import json
import logging
import logging as logger
import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from json import loads

import requests
from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata

from sakura.core.S3Uploader import S3Uploader
from sakura.nino.KafkaBatchResolvedTrackDurationCollector import KafkaBatchResolvedTrackDurationCollector
from sakura.nino.Mp3PreviewGeneratorAudioProcessor import Mp3PreviewGeneratorAudioProcessor
from sakura.nino.TrackDurationResolverAudioProcessor import TrackDurationResolverAudioProcessor
from sakura.nino.audio_cutter import AudioCutter

# Simple script that just listen to events from kafka,
# on received event delegates the event to AudioProcessor(s)
cutter = AudioCutter()
logging.getLogger().setLevel(logging.INFO)
executor = ThreadPoolExecutor(max_workers=28, thread_name_prefix="[nino-mp3]")


s3Uploader = S3Uploader(
    client_id=os.environ.get("aws_client_id"),
    client_secret=os.environ.get("aws_client_secret"),
    bucket_name=os.environ.get("aws_s3_bucket_name"),
    bucket_region=os.environ.get("aws_s3_bucket_region")
)

consumer = KafkaConsumer("albums-event-warehouse",
                         bootstrap_servers=['localhost:29092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group123',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))


def serializer(msg):
    return json.dumps(msg).encode('utf-8')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=serializer
)

track_duration_collector = KafkaBatchResolvedTrackDurationCollector(kafka_producer=producer)

# Chain that will be called to handle the given track
audio_processors = [
    Mp3PreviewGeneratorAudioProcessor(s3Uploader, producer),
    TrackDurationResolverAudioProcessor(track_duration_collector)
]


def handle_event(msg):
    topic_partition = TopicPartition("albums-event-warehouse", msg.partition)
    offset = OffsetAndMetadata(msg.offset + 1, None)
    topic_offset = {topic_partition: offset}

    event_body = msg.value.get('body', {})
    tracks = event_body.get('uploadedTracks', {}).get('items', [{}])
    for track in tracks:
        track_uri = track.get("uri")

        if track_uri is None:
            logger.warning(f"Null value in track uri. {track}")
            continue

        album_id = event_body["id"]

        response = requests.get(track_uri)

        if response.status_code != 200:
            logger.warning(f"There is no existing track uri. No 200 status code. {track},"
                           f" actual status code is: {response.status_code}")
            continue

        def on_complete():
            track_duration_collector.on_complete(album_id=album_id)
            consumer.commit(topic_offset)

        for audio_processor in audio_processors:
            try:
                logger.info(f"Process the audio track: {track_uri} with {audio_processor.__str__()}")
                executor.submit(
                    lambda processor=audio_processor: processor.process_audio_files(BytesIO(response.content), track, msg)
                ).add_done_callback(lambda f: on_complete())

            except Exception as err:
                logger.error(
                    f"There is a problem with processing the audio track {track},"
                    f" the problem occurred at {audio_processor}",
                    err)


for message in consumer:
    try:
        executor.submit(handle_event(message))

    except Exception as ex:
        logger.error("Failed to process the following record: ", message, ex)
