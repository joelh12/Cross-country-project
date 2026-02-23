from bs4 import BeautifulSoup

from scraper.results import extract_athlete_name


def test_extract_athlete_name_uses_first_last_spans():
    html = """
    <a href="athlete.php?id=123">
      <span class="firstname">Johannes Høsflot</span>
      <span class="lastname">Klæbo</span>
    </a>
    """
    link = BeautifulSoup(html, "lxml").find("a")
    assert link is not None

    name = extract_athlete_name(link)
    assert name == "Johannes Høsflot Klæbo"


def test_extract_athlete_name_fallback_inserts_separator():
    html = """
    <a href="athlete.php?id=123">
      <span>Klæbo</span><span>Johannes Høsflot</span>
    </a>
    """
    link = BeautifulSoup(html, "lxml").find("a")
    assert link is not None

    name = extract_athlete_name(link)
    assert name == "Klæbo Johannes Høsflot"

