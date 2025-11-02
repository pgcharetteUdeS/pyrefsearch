"""search_openalex.py

    Search OpenAlex database

    https://github.com/J535D165/pyalex

"""

__all__ = [
    "query_author_homonyms_openalex",
    "query_author_profiles_by_id_openalex",
    "query_publications_openalex",
    "config_openalex",
]

import itertools
import html
import pandas as pd
from pyalex import config, Authors, Works
from referencequery import ReferenceQuery
import re
import requests
from utils import (
    console,
    count_publications_by_type_in_df,
    to_lower_no_accents_no_hyphens,
)


def config_openalex():
    config.email = "paul.charette@usherbrooke.ca"
    config.max_retries = 5
    config.retry_backoff_factor = 0.5


def query_author_profiles_by_id_openalex(
    reference_query: ReferenceQuery,
) -> pd.DataFrame:
    """

    Fetch author profiles from their IDs in the OpenAlex database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object

    Returns: DataFrame with author profiles

    """

    data_rows: list = []
    for [name, openalex_id] in zip(
        reference_query.au_names, reference_query.openalex_ids
    ):
        author = Authors()[openalex_id]
        if not author:
            console.print(
                f"[red]ERREUR: Aucun résultat dans OpenAlex pour l'identifiant '{openalex_id}'![/red]"
            )
        data_rows.append(
            [
                name[0],
                name[1],
                author["display_name"],
                f'=HYPERLINK("{author["id"]}")',
                f'=HYPERLINK("{author["orcid"]}")' if author["orcid"] else "",
                author["works_count"],
                [
                    last_inst["display_name"]
                    for last_inst in author["last_known_institutions"]
                ],
            ]
        )

    author_profiles: pd.DataFrame = pd.DataFrame(
        data_rows,
        columns=[
            "Nom de famille",
            "Prénom",
            "OpenAlex - display_name",
            "Profil OpenAlex",
            "Profil ORCID",
            "Publications",
            "Institutions",
        ],
    )

    # Check for errors
    author_profiles.drop("OpenAlex - display_name", axis=1, inplace=True)

    return author_profiles


def _flag_matched_openalex_author_ids_and_affiliations(
    reference_query: ReferenceQuery, author_profiles: pd.DataFrame
) -> pd.DataFrame:
    """
    Flag author profiles with local affiliations and matching OpenAlex IDs between
    input Excel file and Scopus database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        author_profiles (pd.DataFrame): DataFrame with author profiles

    Returns: DataFrame with local author profiles flagged

    """

    def set_affiliation_and_id(row) -> str | None:
        if row["Last known institutions"] is None and row["Affiliations"] is None:
            return None
        last_known_institutions_match: bool = any(
            any(
                local_affiliation["name"] in to_lower_no_accents_no_hyphens(institution)
                for institution in row["Last known institutions"]
            )
            for local_affiliation in reference_query.local_affiliations
        )
        au_id_index: int | None = reference_query.au_id_to_index.get(
            re.search(r"A\d{10}", row["OpenAlex profile"]).group()
        )
        au_id_match: bool = au_id_index is not None and to_lower_no_accents_no_hyphens(
            reference_query.au_names[au_id_index][0]
        ) == to_lower_no_accents_no_hyphens(row["Surname"])
        if last_known_institutions_match and au_id_match:
            return "Affl. + ID"
        elif last_known_institutions_match:
            return "Affl."
        elif au_id_match:
            return "ID"
        else:
            return None

    # Add the "Affl/ID" to the input dataframe
    reference_query.au_id_to_index = {
        au_id: index for index, au_id in enumerate(reference_query.openalex_ids)
    }
    author_profiles["Affl/ID"] = author_profiles.apply(set_affiliation_and_id, axis=1)

    # Reposition the "Affl/ID" column
    affl_id_column = author_profiles.pop("Affl/ID")
    author_profiles.insert(3, "Affl/ID", affl_id_column)

    return author_profiles


def query_author_homonyms_openalex(
    reference_query: ReferenceQuery,
) -> pd.DataFrame:
    """
    Fetch author profiles from the OpenAlex database by name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns : DataFrame with author profiles
    """

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
                author["display_name"],
                f'=HYPERLINK("{author["id"]}")',
                author["created_date"],
                f'=HYPERLINK("{author["orcid"]}")' if author["orcid"] else "",
                author["works_count"],
                [
                    last_inst["display_name"]
                    for last_inst in author["last_known_institutions"]
                ],
                [
                    affiliation["institution"]["display_name"]
                    for affiliation in author["affiliations"]
                ],
                [topic["display_name"] for topic in author["topics"]],
            ]
            for author in author_search_results
        )
        data_rows.extend([""])

    # Build dataframe from data
    author_profiles: pd.DataFrame = pd.DataFrame(
        data_rows,
        columns=[
            "Surname",
            "Given name",
            "Display name",
            "OpenAlex profile",
            "Date created",
            "ORCID profile",
            "Count",
            "Last known institutions",
            "Affiliations",
            "Topics",
        ],
    )

    # Add "Affl/ID" column
    author_profiles = _flag_matched_openalex_author_ids_and_affiliations(
        reference_query=reference_query, author_profiles=author_profiles
    )

    return author_profiles


def _get_publication_info_from_crossref(doi) -> dict | None:
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
        timeout=30,
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
            "authors": [
                f"{author['family'] if 'family' in author else ''}, "
                f"{author['given'] if 'given' in author else ''}"
                for author in data["message"]["author"]
            ],
            "Affiliations": list(
                itertools.chain(
                    *[
                        [
                            affiliation["name"]
                            for affiliation in authors["affiliation"]
                            if "name" in affiliation
                        ]
                        for authors in data["message"]["author"]
                    ]
                )
            ),
            "volume": (
                data["message"]["volume"] if "volume" in data["message"] else None
            ),
            "issue": data["message"]["issue"] if "issue" in data["message"] else None,
        }
        if data and "message" in data
        else None
    )


def _add_local_author_name_and_count_columns(
    publications: pd.DataFrame,
) -> pd.DataFrame:

    # Columns to use for removing publication duplicates
    match_criteria_columns: list[str] = ["title", "subtype", "doi"]

    # Build DataFrame with unique publication entries
    publications_duplicate_counts = publications.value_counts(match_criteria_columns)
    publications_without_duplicate: pd.DataFrame = pd.DataFrame(
        publications_duplicate_counts.index.tolist()
    )
    publications_without_duplicate.columns = match_criteria_columns
    publications_without_duplicate["duplicates"] = (
        publications_duplicate_counts.values.tolist()
    )
    publications_without_duplicate.reset_index()

    # Add column of duplicate indices
    def find_duplicate_indices(row) -> list[int]:
        indices: list = [
            index
            for index, row_all in publications.iterrows()
            if row_all["title"] == row["title"]
            and row_all["subtype"] == row["subtype"]
            and row_all["doi"] == row["doi"]
        ]
        return indices

    publications_without_duplicate["Duplicate indices"] = (
        publications_without_duplicate.apply(find_duplicate_indices, axis=1)
    )

    # Add column of local authors
    def list_local_authors(row) -> list[str]:
        local_authors = [
            publications.iloc[index]["Membre3IT"] for index in row["Duplicate indices"]
        ]
        return local_authors

    publications_without_duplicate["Auteurs locaux"] = (
        publications_without_duplicate.apply(list_local_authors, axis=1)
    )

    # Add local author count column (for n > 1 to indicate joint publications
    def count_local_coauthors(row) -> int | None:
        return len(row["Auteurs locaux"]) if len(row["Auteurs locaux"]) > 1 else None

    publications_without_duplicate["Collab interne"] = (
        publications_without_duplicate.apply(count_local_coauthors, axis=1)
    )

    # Add missing columns to the output dataframe
    columns_missing: list[str] = [
        item
        for item in publications.columns.tolist()
        if item not in match_criteria_columns
    ]
    for column in columns_missing:
        publications_without_duplicate[column] = [
            publications.iloc[row["Duplicate indices"][0]][column]
            for _, row in publications_without_duplicate.iterrows()
        ]

    # Replace "posted-content" with "preprint" subtype
    publications_without_duplicate.loc[
        publications_without_duplicate["subtype"] == "posted-content", "subtype"
    ] = "preprint"

    # Drop temporary columns
    publications_without_duplicate.drop(
        ["duplicates", "Duplicate indices"], axis=1, inplace=True
    )

    return publications_without_duplicate


def query_publications_openalex(
    reference_query: ReferenceQuery,
) -> tuple[pd.DataFrame, list[list[int | None]]]:
    """
    Fetch publications for range of years in OpenAlex database for list of author IDs

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns : DataFrame with publication search results (pd.DataFrame),
              list of publication type counts by author (list)
    """

    # Loop though authors to fetch/process records
    pub_type_counts_by_author: list = []
    publications = pd.DataFrame([])
    for openalex_id, author_name in zip(
        reference_query.openalex_ids, reference_query.au_names
    ):
        if openalex_id:
            works = Works().filter(
                author={"id": openalex_id},
                publication_year=f"{reference_query.pub_year_first}-{reference_query.pub_year_last}",
            )
            works_df = pd.DataFrame([])
            for work in itertools.chain(*works.paginate(per_page=200, n_max=None)):
                # Fetch OpenAlex record
                title_openalex: str = work["title"]
                type_openalex: str = work["type"]
                date_openalex: str = work["publication_date"]
                publication_name_openalex = (
                    work["primary_location"]["source"]["display_name"]
                    if "primary_location" in work
                    and work["primary_location"]["source"]
                    and "display_name" in work["primary_location"]["source"]
                    else None
                )
                if "authorships" in work:
                    authors_openalex = [
                        author["author"]["display_name"] if "author" in author else ""
                        for author in work["authorships"]
                    ]
                    author_institutions_openalex = [
                        (
                            [
                                institution["display_name"]
                                for institution in author["institutions"]
                            ]
                            if "institutions" in author
                            else []
                        )
                        for author in work["authorships"]
                    ]
                    author_affiliations_openalex = [
                        (
                            [
                                html.unescape(affiliations["raw_affiliation_string"])
                                for affiliations in author["affiliations"]
                            ]
                            if "affiliations" in author
                            else []
                        )
                        for author in work["authorships"]
                    ]
                else:
                    authors_openalex = []
                    author_institutions_openalex = []
                    author_affiliations_openalex = []

                # Fetch Crossref record
                if publication_info_from_crossref := _get_publication_info_from_crossref(
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
                    work_type: str = type_crossref or (type_openalex or "Other")
                    work_title: str = title_openalex or title_openalex
                    work_publication_name: str | None = (
                        publication_name_crossref or publication_name_openalex
                    )

                    # Add the record to the dataframe for this author
                    works_df = pd.concat(
                        [
                            works_df,
                            pd.DataFrame(
                                {
                                    "title": [work_title],
                                    "subtype": [work_type],
                                    "coverDate": [date_openalex],
                                    "Membre3IT": [f"{author_name[1]} {author_name[0]}"],
                                    "author_names": [authors_openalex],
                                    "institutions": [author_institutions_openalex],
                                    "affiliations": [author_affiliations_openalex],
                                    "publicationName": [work_publication_name],
                                    "volume": [volume],
                                    "doi": [f'=HYPERLINK("{work["doi"]}")'],
                                    "id": work["id"],
                                }
                            ),
                        ],
                        ignore_index=True,
                    )
                    pub_type_counts_by_author.append(
                        count_publications_by_type_in_df(
                            publication_type_codes=reference_query.publication_type_codes,
                            df=works_df,
                        )
                    )

            # Add the dataframe for this author to the dataframe of all p
            if not works_df.empty:
                publications = pd.concat([publications, works_df])

    # Remove duplicates and add local author name and count columns
    publications.reset_index(drop=True, inplace=True)
    publications = _add_local_author_name_and_count_columns(publications=publications)

    # Sort by title (date is unreliable)
    publications = publications.sort_values(by=["title"])
    publications.reset_index(drop=True, inplace=True)

    # Reformat pub_type_counts_by_author list
    pub_type_counts_by_author_transpose: list = [
        list(row) for row in zip(*pub_type_counts_by_author)
    ]

    return publications, pub_type_counts_by_author_transpose
