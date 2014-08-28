from __future__ import unicode_literals

import functools
import logging

from mopidy import audio, backend

import spotify


logger = logging.getLogger(__name__)


def need_data_callback(spotify_backend, length_hint):
    spotify_backend.playback.on_need_data(length_hint)


def enough_data_callback(spotify_backend):
    spotify_backend.playback.on_enough_data()


def seek_data_callback(spotify_backend, time_position):
    spotify_backend.playback.on_seek_data(time_position)


class SpotifyPlaybackProvider(backend.PlaybackProvider):
    # These GStreamer caps matches the audio data provided by libspotify
    _caps = (
        'audio/x-raw-int, endianness=(int)1234, channels=(int)2, '
        'width=(int)16, depth=(int)16, signed=(boolean)true, '
        'rate=(int)44100')

    def __init__(self, *args, **kwargs):
        super(SpotifyPlaybackProvider, self).__init__(*args, **kwargs)
        self._timeout = self.backend._config['spotify']['timeout']

        self._buffer_timestamp = 0
        self._first_seek = False
        self._push_audio_data = True

        self.backend._session.on(
            spotify.SessionEvent.MUSIC_DELIVERY, self._on_music_delivery)
        self.backend._session.on(
            spotify.SessionEvent.END_OF_TRACK, self._on_end_of_track)

    def _on_music_delivery(self, session, audio_format, frames, num_frames):

        # TODO This is called from an internal libspotify thread, thus it
        # should not change actor state directly.

        if not self._push_audio_data:
            return 0

        known_format = (
            audio_format.sample_type == spotify.SampleType.INT16_NATIVE_ENDIAN)
        assert known_format, 'Expects 16-bit signed integer samples'

        capabilites = """
            audio/x-raw-int,
            endianness=(int)1234,
            channels=(int)%(channels)d,
            width=(int)16,
            depth=(int)16,
            signed=(boolean)true,
            rate=(int)%(sample_rate)d
        """ % {
            'sample_rate': audio_format.sample_rate,
            'channels': audio_format.channels,
        }

        duration = audio.calculate_duration(
            num_frames, audio_format.sample_rate)
        buffer_ = audio.create_buffer(
            bytes(frames), capabilites=capabilites,
            timestamp=self._buffer_timestamp, duration=duration)

        self._buffer_timestamp += duration

        if self.audio.emit_data(buffer_).get():
            return num_frames
        else:
            return 0

    def _on_end_of_track(self, session):
        logger.debug('End of track reached')
        self.audio.emit_end_of_stream()

    def play(self, track):
        if track.uri is None:
            return False

        spotify_backend = self.backend.actor_ref.proxy()
        need_data_callback_bound = functools.partial(
            need_data_callback, spotify_backend)
        enough_data_callback_bound = functools.partial(
            enough_data_callback, spotify_backend)
        seek_data_callback_bound = functools.partial(
            seek_data_callback, spotify_backend)

        self._first_seek = True

        try:
            sp_track = self.backend._session.get_track(track.uri)
            sp_track.load(self._timeout)
            self.backend._session.player.load(sp_track)
            self.backend._session.player.play()
            self._buffer_timestamp = 0

            self.audio.prepare_change()
            self.audio.set_appsrc(
                self._caps,
                need_data=need_data_callback_bound,
                enough_data=enough_data_callback_bound,
                seek_data=seek_data_callback_bound)
            self.audio.start_playback()
            self.audio.set_metadata(track)

            return True
        except spotify.Error as exc:
            logger.info('Playback of %s failed: %s', track.uri, exc)
            return False

    def resume(self):
        self.backend._session.player.play()
        return super(SpotifyPlaybackProvider, self).resume()

    def stop(self):
        self.backend._session.player.pause()
        return super(SpotifyPlaybackProvider, self).stop()

    def on_need_data(self, length_hint):
        logger.debug('playback.on_need_data(%d) called', length_hint)
        self._push_audio_data = True

    def on_enough_data(self):
        logger.debug('playback.on_enough_data() called')
        self._push_audio_data = False

    def on_seek_data(self, time_position):
        logger.debug('playback.on_seek_data(%d) called', time_position)

        if time_position == 0 and self._first_seek:
            self._first_seek = False
            logger.debug('Skipping seek due to issue mopidy/mopidy#300')
            return

        self._buffer_timestamp = audio.millisecond_to_clocktime(time_position)
        self.backend._session.player.seek(time_position)
