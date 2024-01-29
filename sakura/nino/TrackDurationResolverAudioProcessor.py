import logging as logger
from io import BytesIO

from pydub import AudioSegment

from sakura.nino.AudioProcessor import AudioProcessor
from sakura.nino.ResolvedTrackDuration import ResolvedTrackDuration


# Resolves the track duration from track and push event to kafka
class TrackDurationResolverAudioProcessor(AudioProcessor):

    def __init__(self, track_duration_collector):
        self.track_duration_collector = track_duration_collector

    def process_audio_files(self, audio_file_bytes: BytesIO, track_info, event):

        logger.info(f"Resolving the length of track: {track_info}")
        try:
            segment = AudioSegment.from_file(audio_file_bytes)
            event_body = event.value.get('body', {})
            album_id = event_body.get("id")

            track_length_ms = len(segment)

            track_duration_info = ResolvedTrackDuration(track_info.get("id"), track_length_ms)
            self.track_duration_collector.on_next(track_duration=track_duration_info, album_id=album_id)
            logger.info(f"Successfully processed the: {track_info}.")
        except Exception as err:
            logger.error(f"Error while processing the: {track_info}", err)
