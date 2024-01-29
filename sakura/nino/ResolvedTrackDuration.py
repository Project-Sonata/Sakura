class ResolvedTrackDuration:
    track_id: str
    duration_ms: int

    def __init__(self, track_id: str, duration_ms: int):
        self.track_id = track_id
        self.duration_ms = duration_ms
