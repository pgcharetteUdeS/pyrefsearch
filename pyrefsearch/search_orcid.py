"""search_orcid.py

Search ORCID database

"""

__all__ = [
    "query_publications_orcid",
]

import requests

# Public API base URL and search endpoint
BASE_URL = "https://pub.orcid.org/v3.0/search/"


# You can perform simple searches without client credentials for public data
# If you are an ORCID member, you can use your client_id and client_secret for broader access


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


def query_publications_orcid(family_name: str, given_name: str):
    if results := _search_orcid(
        f"family-name:{family_name} AND given-names:{given_name}"
    ):
        print(f"Found {results.get('num-found', 0)} result(s).")
        for record in results.get("result", []):
            orcid_id = record.get("orcid-identifier", {}).get("uri")
            print(f"ORCID ID: {orcid_id}")
    else:
        print("No results or an error occurred.")
