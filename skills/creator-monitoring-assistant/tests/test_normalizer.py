import unittest
from decimal import Decimal

from app.services.normalizer import normalize_creator_name, parse_int, parse_money


class NormalizerTest(unittest.TestCase):
    def test_normalize_creator_name(self):
        self.assertEqual(normalize_creator_name("  Alice   Shop "), "alice shop")
        self.assertEqual(normalize_creator_name("A\u200bB"), "a b")

    def test_parse_money(self):
        self.assertEqual(parse_money("$1,234.50"), Decimal("1234.50"))
        self.assertEqual(parse_money("invalid"), Decimal("0"))

    def test_parse_int(self):
        self.assertEqual(parse_int("1,234"), 1234)
        self.assertEqual(parse_int("12.7"), 12)
        self.assertEqual(parse_int(""), 0)


if __name__ == "__main__":
    unittest.main()

