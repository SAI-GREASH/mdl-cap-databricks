
import importlib.util
import unittest
from pathlib import Path

# Load our existing data generator without running main()
generator_path = (
    Path(__file__).resolve().parents[1]
    / "src/data_generation/generate_marketing_data.py"
)

spec = importlib.util.spec_from_file_location(
    "marketing_generator", generator_path
)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class TestMarketingDataGeneration(unittest.TestCase):

    def test_records_per_source(self):
        for source in generator.SOURCES:
            rows = generator.generate_campaign_rows(source)
            self.assertEqual(len(rows), 300)

    def test_valid_marketing_metrics(self):
        rows = generator.generate_campaign_rows("google_ads")

        for row in rows:
            self.assertGreaterEqual(row["impressions"], row["clicks"])
            self.assertGreaterEqual(row["clicks"], row["conversions"])
            self.assertGreaterEqual(row["spend"], 0)

    def test_date_range(self):
        rows = generator.generate_campaign_rows("google_ads")

        self.assertEqual(rows[0]["campaign_date"], "2026-10-01")
        self.assertEqual(rows[-1]["campaign_date"], "2026-10-30")


if __name__ == "__main__":
    unittest.main()
