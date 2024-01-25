from io import BytesIO

"""
Interface to handle the audio tracks received in event.
"""


class AudioProcessor:

    def process_audio_files(self, audio_file_bytes: BytesIO, track_info, event):
        """
        :param audio_file_bytes: audio file represented in bytes
        :param track_info - info about track associated with the given bytes
        :param event: original event that invoked this audio processor, typically event from kafka
        :return: returns nothing
        """
        pass
