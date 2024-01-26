from io import BytesIO

from pydub import AudioSegment

from sakura.nino.AudioProcessor import AudioProcessor
import logging as logger


# Resolves the track duration from track and push event to kafka
class TrackDurationResolverAudioProcessor(AudioProcessor):
    headers = [
        ("event_type", bytes("track_length_resolved", encoding="UTF-8")),
        ("content_type", bytes("application/json", encoding="UTF-8"))
    ]

    def __init__(self, kafka_producer):
        self.kafka_producer = kafka_producer

    def process_audio_files(self, audio_file_bytes: BytesIO, track_info, event):

        logger.info(f"Resolving the length of track: {track_info}")
        try:
            segment = AudioSegment.from_file(audio_file_bytes)
            event_body = event.value.get('body', {})
            album_id = event_body.get("id")

            track_length_ms = len(segment)

            body = {"album_id": album_id, "track_id": track_info.get("id"), "length_ms": track_length_ms}

            self.kafka_producer.send(topic="albums-event-warehouse", value=body, headers=self.headers)
            logger.info(f"Successfully processed the: {track_info}. Sent the response to kafka with payload: {body}")
        except Exception as err:
            logger.error(f"Error while processing the: {track_info}", err)
