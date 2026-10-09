from __future__ import annotations

from datetime import date

from dx_productivity.verify.published import parse_release_notes, parse_release_page

# Shapes copied from the official pages (October 2026), trimmed to a few entries.
KAFKA = """<h3 id=411>4.1.1</h3><ul><li>Released November 12, 2025</li></ul>
<h3>4.1.0</h3><ul><li>Released September 2, 2025</li></ul><h3>3.2.3</h3><p>Released Sept 19, 2022</p>
<h3>0.11.0.2</h3><p>Released November 17, 2017</p>"""
SPARK = """<h3 class="entry-title"><a href="/news/spark-4-0-1-released.html">Spark 4.0.1 released</a>
</h3><div class="entry-date">September 6, 2025</div>"""
FLINK = """<h3 id="flink-210---2025-07-31">Flink 2.1.0 - 2025-07-31 <a href="#">#</a></h3>"""


def test_kafka_downloads_page() -> None:
    parsed = parse_release_page("kafka_downloads", KAFKA)
    assert parsed["4.1.1"] == date(2025, 11, 12)
    assert parsed["4.1.0"] == date(2025, 9, 2)
    assert parsed["3.2.3"] == date(2022, 9, 19)  # "Sept" abbreviation
    assert "11.0.2" not in parsed  # 0.11.0.2 is not misread as 11.0.2


def test_spark_news_and_flink_downloads_pages() -> None:
    assert parse_release_page("spark_news", SPARK) == {"4.0.1": date(2025, 9, 6)}
    assert parse_release_page("flink_downloads", FLINK) == {"2.1.0": date(2025, 7, 31)}


def test_release_notes_count_distinct_keys() -> None:
    notes = "<li>[<a>KAFKA-1</a>] - x</li><li>[<a>KAFKA-22</a>]</li><li>KAFKA-1 again</li><li>SPARK-3</li>"
    assert parse_release_notes(notes, "KAFKA") == {"KAFKA-1", "KAFKA-22"}
