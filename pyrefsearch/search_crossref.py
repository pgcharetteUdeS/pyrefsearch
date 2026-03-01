"""search_crossref.py

Search crossref database

"""

__all__ = ["get_publication_info_from_crossref"]

import itertools
import html
import requests


def get_publication_info_from_crossref(doi) -> dict:
    """
    Retrieves the publication name (journal name) for a given DOI using the Crossref API.

    Args:
        doi (str): The Digital Object Identifier (DOI) of the publication.

    Returns:
        str or None: The name of the publication (journal) if found, otherwise None.
    """

    """
    # Use habanero library for crossref queries
    from habanero import Crossref
    if not hasattr(_get_publication_info_from_crossref, "crossref"):
        _get_publication_info_from_crossref.crossref = Crossref()  # type: ignore[attr-defined]
    try:
        data = _get_publication_info_from_crossref.crossref.works(ids=f"{doi}")  # type: ignore[attr-defined]
    except Exception as e:
        return None
    """

    empty_dict: dict = {
        "title": None,
        "type": None,
        "publication_name": None,
        "authors": [],
        "affiliations": [],
        "volume": None,
        "issue": None,
    }

    response = requests.get(
        f"https://api.crossref.org/works/{doi}",
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if not response:
        return empty_dict
    data = response.json()
    return (
        {
            "title": data["message"]["title"],
            "type": data["message"]["type"],
            "publication_name": (
                data["message"]["container-title"][0]
                if data["message"]["container-title"]
                else None
            ),
            "authors": (
                [
                    f"{author['family'] if 'family' in author else ''}, "
                    f"{author['given'] if 'given' in author else ''}"
                    for author in data["message"]["author"]
                ]
                if "author" in data["message"]
                else []
            ),
            "affiliations": (
                list(
                    itertools.chain(
                        *[
                            [
                                html.unescape(affiliation["name"])
                                for affiliation in authors["affiliation"]
                                if "name" in affiliation
                            ]
                            for authors in data["message"]["author"]
                        ]
                    )
                )
                if "author" in data["message"]
                else []
            ),
            "volume": (
                data["message"]["volume"] if "volume" in data["message"] else None
            ),
            "issue": data["message"]["issue"] if "issue" in data["message"] else None,
        }
        if data and "message" in data
        else empty_dict
    )
