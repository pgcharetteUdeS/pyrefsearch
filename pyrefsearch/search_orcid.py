"""search_orcid.py

Search ORCID database

"""

__all__ = [
    "query_author_id_by_name_orcid",
    "query_publications_by_id_orcid",
]

import requests

# Public API base URL and search endpoint
BASE_URL = "https://pub.orcid.org/v3.0/search/"


# You can perform simple searches without client credentials for public data
# If you are an ORCID member, you can use your client_id and client_secret for broader access

import requests


def query_publications_by_id_orcid(
    orcid_id: str, start_year: int, end_year: int
) -> list[dict]:
    """
    Fetch works from ORCID for a given ORCID ID, optionally filtered by year range.

    Args:
        orcid_id:   ORCID identifier (e.g. "0000-0002-1825-0097")
        start_year: Filter works published from this year (inclusive)
        end_year:   Filter works published up to this year (inclusive)

    Returns:
        List of work dicts with title, year, type, DOI, and URL.
    """
    url = f"https://pub.orcid.org/v3.0/{orcid_id}/works"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    groups = data.get("group", [])
    works = []

    for group in groups:
        # Each group can have multiple work summaries; take the preferred/first one
        summaries = group.get("work-summary", [])
        if not summaries:
            continue
        work = summaries[0]

        # Extract publication year
        pub_date = work.get("publication-date") or {}
        year_val = pub_date.get("year", {})
        year = (
            int(year_val.get("value")) if year_val and year_val.get("value") else None
        )

        # Apply year filter
        if start_year and year and year < start_year:
            continue
        if end_year and year and year > end_year:
            continue

        # Extract title
        title_obj = work.get("title", {}) or {}
        title = title_obj.get("title", {}).get("value", "N/A")

        # Extract DOI and ORCID work URL
        doi = None
        external_ids = (work.get("external-ids") or {}).get("external-id", [])
        for ext_id in external_ids:
            if ext_id.get("external-id-type") == "doi":
                doi = ext_id.get("external-id-value")
                break

        works.append(
            {
                "title": title,
                "year": year,
                "type": work.get("type"),
                "doi": doi,
                "doi_url": f"https://doi.org/{doi}" if doi else None,
                "orcid_url": (
                    work.get("url", {}).get("value") if work.get("url") else None
                ),
                "put_code": work.get("put-code"),
            }
        )

    # Sort by year descending
    works.sort(key=lambda w: w["year"] or 0, reverse=True)

    print(f"Found {len(works)} works\n")
    for w in works:
        print(f"[{w['year']}] {w['title']}")
        if w["doi_url"]:
            print(f"        DOI: {w['doi_url']}")
        print()

    return works


def _search_orcid(query):
    """
    Searches the ORCID public registry using the API.
    Query syntax is based on SOLR.
    """
    # Define headers to accept JSON
    headers = {"Accept": "application/vnd.orcid+json"}

    # Construct the full URL with the query parameter
    # Example query: 'family-name:Smith AND given-names:John'
    params = {"q": query}

    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes (4XX or 5XX)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def query_author_id_by_name_orcid(family_name: str, given_name: str) -> list | None:
    if results := _search_orcid(
        f"family-name:{family_name} AND given-names:{given_name}"
    ):
        print(f"Found {results.get('num-found', 0)} result(s).")
        for record in results.get("result", []):
            orcid_id = record.get("orcid-identifier", {}).get("uri")
            print(f"ORCID ID: {orcid_id}")
        return results.get("result", [])
    else:
        print("No results or an error occurred.")
        return None