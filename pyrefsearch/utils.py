"""utils.py

Various utility functions

"""

__all__ = [
    "Colors",
    "console",
    "count_publications_by_type_in_df",
    "remove_middle_initial",
    "tabulate_patents_per_author",
    "to_lower_no_accents_no_hyphens",
]

from functools import lru_cache
import pandas as pd
import re
from rich.console import Console
from unidecode import unidecode


class Colors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    YELLOW = "\033[93m"
    BOLD = "\033[1m"
    ITALICS = "\033[3m"
    UNDERLINE_START = "\033[4m"


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

    return unidecode(s.lower().strip()).replace("-", " ").replace("ç", "c")


def remove_middle_initial(full_name):
    # Matches a space, followed by a single uppercase letter (optionally followed by a period),
    # and then another space. This targets middle initials.
    return re.sub(r"\s[A-Za-z]\.?\s", " ", full_name)


def count_publications_by_type_in_df(
    publication_type_codes: list, df: pd.DataFrame
) -> list:
    """
    Count number of publications by type in a dataframe

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        df (pd.DataFrame): DataFrame with publications

    Returns: List of counts per publication type

    """

    if df.empty:
        return [None] * len(publication_type_codes)
    else:
        return [
            (
                len(df[df["subtype"] == pub_type])
                if len(df[df["subtype"] == pub_type]) > 0
                else None
            )
            for pub_type in publication_type_codes
        ]


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
