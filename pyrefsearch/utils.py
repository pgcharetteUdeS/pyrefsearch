"""utils.py

Various utility functions

"""
__all__ = ["to_lower_no_accents_no_hyphens"]

from functools import lru_cache
from unidecode import unidecode


@lru_cache(maxsize=1024)
def to_lower_no_accents_no_hyphens(s: str) -> str:
    """
    Convert string to lower case and remove accents and hyphens

    Args:
        s (str): Input string

    Returns: String in lower case without accents

    """

    return unidecode(s.replace("-", " ").lower().strip())


