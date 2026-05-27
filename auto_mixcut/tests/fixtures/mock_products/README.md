# Mock Product Fixtures

The automated tests generate tiny placeholder `.mp4` files at runtime and run with `AUTO_MIXCUT_MOCK_FFMPEG=1`.

This intentionally keeps the test suite independent of real product素材 and independent of a local FFmpeg install, while the production media layer still shells out to `ffprobe`/`ffmpeg`.
