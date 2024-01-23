from io import BytesIO

import requests
from pydub import AudioSegment


# Cut the audio tracks
class AudioCutter:

    def __init__(self):
        pass

    def cut_local_file(self, path, output, start, end):
        audio = AudioSegment.from_file(path)

        cut_audio = audio[start:end]

        cut_audio.export(output)

    def cut_from_web(self, url, output, start, end):
        response = requests.get(url)
        content_bytes = response.content

        audio = AudioSegment.from_file(BytesIO(content_bytes))

        cut_audio = audio[start:end]
        cut_audio.export(output)

    def cut_from_web_and_return_bytes(self, url, start, end):
        response = requests.get(url)
        content_bytes = response.content

        audio = AudioSegment.from_file(BytesIO(content_bytes))

        cut_audio = audio[start:end]

        bytes_to_return = BytesIO()

        cut_audio.export(bytes_to_return)

        return bytes_to_return
