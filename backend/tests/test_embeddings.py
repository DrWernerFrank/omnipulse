import pytest

from app.embeddings import cosine, embed


def test_embed_dimension():
    assert len(embed("hello world")) == 256


def test_embed_identical():
    a = embed("Fed raises rates")
    b = embed("Fed raises rates")
    assert cosine(a, b) == pytest.approx(1.0, abs=1e-6)


def test_embed_similar_texts_higher_than_unrelated():
    fed1 = embed("Federal Reserve cuts interest rates by 25 basis points")
    fed2 = embed("The Fed cuts rates by a quarter point in a surprise move")
    gpu = embed("NVIDIA announces new Blackwell GPU with 208 billion transistors")
    assert cosine(fed1, fed2) > cosine(fed1, gpu)


def test_embed_empty():
    assert embed("") == [0.0] * 256
    assert cosine(embed(""), embed("x")) == 0.0
