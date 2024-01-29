from sakura.nino.ResolvedTrackDuration import ResolvedTrackDuration


# Collect the mp3 previews based on album id, when everything is processed - call the
# on_complete(album_id) with album id for which track previews were generated
class ResolvedTrackDurationCollector:

    def on_next(self, track_duration: ResolvedTrackDuration, album_id: str):
        pass

    def on_complete(self, album_id: str):
        pass
