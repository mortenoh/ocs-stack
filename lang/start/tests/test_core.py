import pytest

from playground_lang_start import fizzbuzz, slugify


class TestFizzbuzz:
    def test_first_fifteen(self):
        result = fizzbuzz(15)
        assert len(result) == 15
        assert result[0] == "1"
        assert result[2] == "fizz"
        assert result[4] == "buzz"
        assert result[14] == "fizzbuzz"

    def test_single_element(self):
        assert fizzbuzz(1) == ["1"]

    @pytest.mark.parametrize("n", [0, -1, -100])
    def test_rejects_non_positive(self, n: int):
        with pytest.raises(ValueError, match="must be at least 1"):
            fizzbuzz(n)


class TestSlugify:
    def test_basic(self):
        assert slugify("Hello, World!") == "hello-world"

    def test_collapses_separators(self):
        assert slugify("  a -- b__c  ") == "a-b-c"

    def test_already_slug(self):
        assert slugify("already-a-slug") == "already-a-slug"

    @pytest.mark.parametrize("text", ["", "---", "!!!", "   "])
    def test_rejects_no_alphanumerics(self, text: str):
        with pytest.raises(ValueError, match="no alphanumeric characters"):
            slugify(text)
