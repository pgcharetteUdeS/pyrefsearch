"""search_openalex.py

    Search OpenAlex database

    https://github.com/J535D165/pyalex

"""

__all__ = [
    "query_openalex_author_profiles_by_name",
    "query_openalex_publications",
    "openalex_config",
]

from itertools import chain
import pandas as pd
from pyalex import config, Authors, Works
from referencequery import ReferenceQuery
import requests
from utils import console


def query_openalex_author_profiles_by_name(
    reference_query: ReferenceQuery,
) -> pd.DataFrame:
    """
    Fetch author profiles from the OpenAlex database by name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns : DataFrame with author profiles
    """

    console.print(
        "[green]\n** Recherche d'auteur.e.s dans la base de données OpenAlex **[/green]"
    )

    data_rows: list = []
    for name in reference_query.au_names:
        author_search_results = Authors().search(f"{name[1]} {name[0]}").get()
        if not author_search_results:
            console.print(
                f"[red]ERREUR: Aucun résultat dans OpenAlex pour {name[1]} {name[0]}![/red]"
            )
        data_rows.extend(
            [
                name[0],
                name[1],
                f'=HYPERLINK("{author["id"]}")',
                f'=HYPERLINK("{author["orcid"]}")' if author["orcid"] else "",
                author["works_count"],
                author["display_name"],
                author["created_date"],
                ", ".join(
                    [
                        last_inst["display_name"]
                        for last_inst in author["last_known_institutions"]
                    ]
                ),
                ", ".join(
                    [
                        affiliation["institution"]["display_name"]
                        for affiliation in author["affiliations"]
                    ]
                ),
                ", ".join([topic["display_name"] for topic in author["topics"]]),
            ]
            for author in author_search_results
        )
        data_rows.extend([""])

    return pd.DataFrame(
        data_rows,
        columns=[
            "Surname",
            "Given name",
            "OpenAlex profile",
            "ORCID profile",
            "Works count",
            "Display name",
            "Created date",
            "Last known institutions",
            "Affiliations",
            "Topics",
        ],
    )


def get_publication_info_from_crossref(doi) -> dict | None:
    """
    Retrieves the publication name (journal name) for a given DOI using the Crossref API.

    Args:
        doi (str): The Digital Object Identifier (DOI) of the publication.

    Returns:
        str or None: The name of the publication (journal) if found, otherwise None.
    """
    response = requests.get(
        f"https://api.crossref.org/works/{doi}",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    if not response:
        return None
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
            "authors": "; ".join(
                [
                    f"{author['family'] if 'family' in author else ''}, "
                    f"{author['given'] if 'given' in author else ''}"
                    for author in data["message"]["author"]
                ]
            ),
            "volume": (
                data["message"]["volume"] if "volume" in data["message"] else None
            ),
            "issue": data["message"]["issue"] if "issue" in data["message"] else None,
        }
        if data and "message" in data
        else None
    )


def query_openalex_publications(reference_query: ReferenceQuery) -> pd.DataFrame:
    # Correspondance between types in OpenAlex records and the output Excel file
    type_map = {
        "journal-article": "Articles",
        "proceedings-article": "Confs",
        "book-chapter": "Chap. de livres",
        "preprint": "Pré-impressions",
        "posted-content": "Pré-impressions",
        "Other": "Autre",
    }

    # Loop though authors to fetch/process records
    publications = pd.DataFrame([])
    for openalex_id in reference_query.openalex_ids:
        if openalex_id:
            works = Works().filter(
                author={"id": openalex_id},
                publication_year=f"{reference_query.pub_year_first}-{reference_query.pub_year_last}",
            )
            works_df = pd.DataFrame([])
            for work in chain(*works.paginate(per_page=200, n_max=None)):
                # OpenAlex record
                title_openalex = work["title"]
                type_openalex = work["type"]
                date_openalex = work["publication_date"]
                publication_name_openalex = (
                    work["primary_location"]["source"]["display_name"]
                    if "primary_location" in work
                    and work["primary_location"]["source"]
                    and "display_name" in work["primary_location"]["source"]
                    else None
                )
                authors_openalex = (
                    "; ".join(
                        [
                            (
                                author["author"]["display_name"]
                                if "author" in author
                                else ""
                            )
                            for author in work["authorships"]
                        ]
                    )
                    if "authorships" in work
                    else None
                )

                # Crossref record
                if publication_info_from_crossref := get_publication_info_from_crossref(
                    work["doi"]
                ):
                    type_crossref = publication_info_from_crossref["type"]
                    publication_name_crossref = publication_info_from_crossref[
                        "publication_name"
                    ]
                    volume = publication_info_from_crossref["volume"]
                else:
                    type_crossref = None
                    publication_name_crossref = None
                    volume = None

                # Store record if the publication name is available either in the OpenAlex or Crossref records
                if (
                    publication_name_openalex is not None
                    or publication_name_crossref is not None
                ):
                    # Consolidate OpenAlex & Crossref record fields
                    work_type: list = [
                        type_map.get(
                            type_crossref or (type_openalex or "Other"), "Autre"
                        )
                    ]
                    work_title: list = [title_openalex or title_openalex]
                    work_publication_name: list = [
                        publication_name_crossref or publication_name_openalex
                    ]

                    # Add the record to the dataframe for this author
                    works_df = pd.concat(
                        [
                            works_df,
                            pd.DataFrame(
                                {
                                    "Type": work_type,
                                    "Titre": work_title,
                                    "Date": [date_openalex],
                                    "Auteurs": authors_openalex,
                                    "Publication": work_publication_name,
                                    "Volume": [volume],
                                    "DOI": [work["doi"]],
                                }
                            ),
                        ],
                        ignore_index=True,
                    )

            # Add dataframe for this author to the dataframe of all p
            if not works_df.empty:
                publications = pd.concat([publications, works_df])

    publications = publications.drop_duplicates("Titre").copy()
    publications = publications.sort_values(by=["Titre"])
    publications.reset_index(drop=True, inplace=True)

    return publications


def openalex_config():
    config.email = "paul.charette@usherbrooke.ca"
    config.max_retries = 5
    config.retry_backoff_factor = 0.5
