import os
import time
import logging as logger
import uuid
from io import BytesIO

from sakura.nino.AudioProcessor import AudioProcessor
from sakura.nino.audio_cutter import AudioCutter

"""
Generate the mp3 preview for the provided audio, pushes the event to kafka on success
"""


class Mp3PreviewGeneratorAudioProcessor(AudioProcessor):
    cutter = AudioCutter()

    TEN_SECONDS = 10 * 1000
    FORTY_SECONDS = 40 * 1000

    headers = [
        ("event_type", bytes("mp3_preview_generated", encoding="UTF-8")),
        ("content_type", bytes("application/json", encoding="UTF-8"))
    ]

    CLOUDFRONT_PREFIX = "m/previews/"
    CLOUDFRONT_HOST = os.environ.get("cloudfront_host")

    def __init__(self, s3_uploader, kafka_producer):
        self.s3Uploader = s3_uploader
        self.kafka_producer = kafka_producer

    def process_audio_files(self, audio_file_bytes: BytesIO, track_info, event_message):
        event_body = event_message.value.get('body', {})
        album_id = event_body.get("id")

        start_time = time.time()

        if album_id is None:
            logger.warning(f"Null album id, can't process the given record {event_message}")
            return

        start_cut_track_position = self.TEN_SECONDS
        end_cut_track_position = self.FORTY_SECONDS

        logger.info(f"Starting to cut the {track_info}. "
                    f"Time window is: {end_cut_track_position - start_cut_track_position}"
                    f"Start position is: {start_cut_track_position}, end position is {end_cut_track_position}")

        bytesIO = self.cutter.cut_from_bytes_and_return_bytes(audio_file_bytes,
                                                              start_cut_track_position,
                                                              end_cut_track_position)

        key = self.CLOUDFRONT_PREFIX + uuid.uuid4().hex.upper()[0:22]
        logger.debug("Generated key for preview: ", key)

        self.s3Uploader.uploadFile(key, bytesIO, "audio/mp3")

        end_processing_time = time.time()

        logger.info(f"Successfully cut and uploaded mp3 preview for {track_info.get('uri')}"
                    f" Started at {start_time}, ended at {end_processing_time},"
                    f" total processing time for the given record is {end_processing_time - start_time}")

        body = {"track_id": track_info.get("id"), "album_id": album_id, "preview_url": self.CLOUDFRONT_HOST + key}

        logger.info(
            f"Generated the event body on successful processing. Sending the event to kafka, payload: {body}")
        self.kafka_producer.send(topic="albums-event-warehouse", value=body, headers=self.headers)
