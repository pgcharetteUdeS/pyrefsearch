"""utils.py

Various utility functions

"""

__all__ = ["tabulate_patents_per_author", "to_lower_no_accents_no_hyphens", "console"]

from functools import lru_cache
import pandas as pd
from rich.console import Console
from unidecode import unidecode


# Init rich console
console = Console()


@lru_cache(maxsize=1024)
def to_lower_no_accents_no_hyphens(s: str) -> str:
    """
    Convert string to lower case and remove accents and hyphens

    Args:
        s (str): Input string

    Returns: String in lower case without accents

    """

    return unidecode(s.replace("-", " ").lower().strip())


def tabulate_patents_per_author(
    au_names: list,
    au_ids: list,
    patents: pd.DataFrame,
) -> list:
    """
    Tabulate number of patents or patent applications per author

    Args:
        au_names (pd.Series): author names
        au_ids (pd.Series): author Scopus ids
        patents (pd.DataFrame): patent search results

    Returns: Number of patents or patent applications per author (list)

    """

    if patents.empty:
        return [None] * len(au_ids)

    def inventor_match(inventors) -> bool:
        return any(
            to_lower_no_accents_no_hyphens(lastname)
            in to_lower_no_accents_no_hyphens(inventor)
            and to_lower_no_accents_no_hyphens(firstname)
            in to_lower_no_accents_no_hyphens(inventor)
            for inventor in inventors
        )

    author_patent_counts: list[int | None] = []
    for [lastname, firstname] in au_names:
        count: int = sum(patents["Inventeurs"].apply(inventor_match))
        author_patent_counts.append(count if count > 0 else None)

    return author_patent_counts
