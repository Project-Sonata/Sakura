from json import loads

from kafka import KafkaConsumer

from sakura.nino.audio_cutter import AudioCutter

consumer = KafkaConsumer("albums-event-warehouse",
                         bootstrap_servers=['localhost:29092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group1',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    try:
        track = message.value.get('body', {}).get('uploadedTracks', {}).get('items', [{}])[0]
        trackUri = track.get('uri', None)

        print(message.value)

        if trackUri is None:
            pass

        cutter = AudioCutter()

        cutter.cut_from_web(trackUri, track["name"] + ".mp3", 10 * 1000, 40 * 1000)

    except:
            print("Failed to process the following record: ", message)
