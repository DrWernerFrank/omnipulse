from app.text import (
    canonical_url, hamming, html_to_markdown, leading_sentences, shingles,
    simhash64, strip_html, truncate,
)


def test_canonical_url_strips_tracking():
    raw = "https://example.com/story/a?utm_source=x&id=1&fbclid=abc#section"
    assert canonical_url(raw) == "https://example.com/story/a?id=1"


def test_canonical_url_www_and_case():
    assert canonical_url("https://WWW.Example.Com/A") == canonical_url("https://example.com/A")


def test_simhash_identical_texts():
    a = "Fed cuts rates by 25 basis points in surprise decision"
    b = "Fed cuts rates by 25 basis points in surprise decision"
    assert hamming(simhash64(a), simhash64(b)) == 0


def test_simhash_syndicated_copy_is_close():
    a = "Fed cuts rates by 25 basis points in surprise decision"
    b = "Fed cuts rates by 25 basis points in a surprise decision on Tuesday"
    # near-duplicates land relatively close; exact dup is 0 and unrelated topics far
    assert hamming(simhash64(a), simhash64(b)) <= 14


def test_simhash_different_topics_far_apart():
    a = simhash64("NVIDIA launches new Blackwell GPU architecture for AI workloads")
    b = simhash64("European Central Bank holds interest rates steady amid inflation concerns")
    assert hamming(a, b) > 14


def test_strip_html_removes_scripts_and_nav():
    html = """
    <html><body>
    <nav>Home About Contact</nav>
    <script>tracker(); </script>
    <p>The Federal Reserve cut rates by 25bps.</p>
    <div class="share-buttons">Share on Twitter</div>
    <p>Markets rallied on the news.</p>
    </body></html>
    """
    text = strip_html(html)
    assert "Federal Reserve cut rates" in text
    assert "tracker" not in text
    assert "Share on Twitter" not in text


def test_html_to_markdown_paragraphs():
    html = "<p>First para.</p><p>Second para with more text to survive filters.</p>"
    md = html_to_markdown(html)
    assert "First para." in md
    assert "Second para" in md


def test_leading_sentences():
    text = (
        "This is the first substantial sentence here. "
        "Another meaningful sentence follows the first one. "
        "A third sentence also appears in this paragraph. Fourth."
    )
    out = leading_sentences(text, 2)
    assert len(out) == 2
    assert out[0].startswith("This is the first")


def test_truncate():
    assert truncate("abcdefghij", 5).endswith("…")
    assert truncate("abc", 10) == "abc"


def test_shingles_jaccard_same():
    a = shingles("the quick brown fox jumps over the lazy dog")
    b = shingles("the quick brown fox jumps over the lazy dog today")
    assert len(a & b) > 0
