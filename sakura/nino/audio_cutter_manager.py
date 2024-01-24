import os
import time
from json import loads
from kafka import KafkaConsumer

import uuid
import logging as logger

from sakura.core.S3Uploader import S3Uploader
from sakura.nino.audio_cutter import AudioCutter

# Simple script that just listen to events from kafka,
# on received event just cut the audio to 30 seconds window and then upload it to the AWS S3 Storage.
# On successful it will push event to kafka indicating that mp3 preview has been generated

FORTY_SECONDS = 40 * 1000

TEN_SECONDS = 10 * 1000

cutter = AudioCutter()

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

for message in consumer:
    try:
        start_time = time.time()

        track = message.value.get('body', {}).get('uploadedTracks', {}).get('items', [{}])[0]
        track_uri = track.get('uri', None)

        if track_uri is None:
            logger.warning("Null track uri in event.", message)
            pass

        start_cut_track_position = TEN_SECONDS
        end_cut_track_position = FORTY_SECONDS

        logger.info(f"Starting to cut the {track_uri}. "
                    f"Time window is: {end_cut_track_position - start_cut_track_position}"
                    f"Start position is: {start_cut_track_position}, end position is {end_cut_track_position}")

        bytesIO = cutter.cut_from_web_and_return_bytes(track_uri, start_cut_track_position, end_cut_track_position)

        key = "m/previews/" + uuid.uuid4().hex.upper()[0:22]
        logger.debug("Generated key for preview: ", key)

        s3Uploader.uploadFile(key, bytesIO, "audio/mp3")

        end_processing_time = time.time()

        logger.info(f"Successfully cut and uploaded mp3 preview for {track_uri}"
                    f"Started at {start_time}, ended at {end_processing_time},"
                    f" total processing time for the given record is {end_processing_time - start_cut_track_position}")

    except Exception as ex:
        logger.error("Failed to process the following record: ", message, ex)
