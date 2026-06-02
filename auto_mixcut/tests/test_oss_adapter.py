from __future__ import annotations

import unittest

from auto_mixcut.adapters.oss import _ascii_header_filename


class OSSAdapterTest(unittest.TestCase):
    def test_ascii_header_filename_replaces_non_latin_names(self):
        self.assertEqual(_ascii_header_filename("003_首图.jpeg"), "003___.jpeg")
        self.assertEqual(_ascii_header_filename('bad"name.mp4'), "bad_name.mp4")


if __name__ == "__main__":
    unittest.main()
