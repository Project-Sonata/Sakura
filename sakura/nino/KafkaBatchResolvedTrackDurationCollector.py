from kafka import KafkaProducer
import logging as logger
from sakura.nino.ResolvedTrackDuration import ResolvedTrackDuration
from sakura.nino.ResolvedTrackDurationCollector import ResolvedTrackDurationCollector


# Send collected data as batch when on_complete method is invoked
class KafkaBatchResolvedTrackDurationCollector(ResolvedTrackDurationCollector):
    cache: dict = {}

    headers = [
        ("event_type", bytes("track_length_resolved", encoding="UTF-8")),
        ("content_type", bytes("application/json", encoding="UTF-8"))
    ]

    def __init__(self, kafka_producer: KafkaProducer):
        self.kafka_producer = kafka_producer

    def on_next(self, track_duration: ResolvedTrackDuration, album_id: str):
        if album_id in self.cache:
            logger.info(f"Appending the {album_id} to existing list")
            self.cache.get(album_id).append(track_duration)
        else:
            logger.info(f"Creating new array for: {album_id}")
            self.cache[album_id] = [track_duration]

    def on_complete(self, album_id: str):
        array_of_track_durations = self.cache.get(album_id)
        total_sum: int = 0

        if array_of_track_durations is None:
            logger.warning(f"Nothing is associated with {album_id}")
            return

        body = {"album_id": album_id, "tracks": []}

        for duration in array_of_track_durations:
            track_info = {
                "track_id": duration.track_id,
                "duration_ms": duration.duration_ms
            }
            body["tracks"].append(track_info)
            total_sum += duration.duration_ms

        # topic="albums-event-warehouse", value=body, headers=self.headers
        body["total_duration_ms"] = total_sum

        self.kafka_producer.send(topic="albums-event-warehouse",
                                 value=body,
                                 headers=self.headers) \
            .add_callback(
            logger.info(f"Successfully sent kafka payload {body}")
        )
