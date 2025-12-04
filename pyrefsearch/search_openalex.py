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
    Colors,
    console,
    count_publications_by_type_in_df,
    remove_middle_initial,
    to_lower_no_accents_no_hyphens,
)


def config_openalex():
    config.email = "paul.charette@usherbrooke.ca"
    config.max_retries = 5
    config.retry_backoff_factor = 0.5


def _check_author_name_and_affiliation_correspondance(
    reference_query: ReferenceQuery, author: Authors, name: list[str]
) -> str:

    e: str = ""

    # Check name
    if remove_middle_initial(
        to_lower_no_accents_no_hyphens(author["display_name"])
    ) != remove_middle_initial(
        to_lower_no_accents_no_hyphens(f"{name[1]} {name[0]}")
    ) and all(
        to_lower_no_accents_no_hyphens(alt_name)
        == to_lower_no_accents_no_hyphens(f"{name[1]} {name[0]}")
        for alt_name in author["display_name_alternatives"]
    ):
        e += "NAME"
        console.print(
            f"{Colors.YELLOW}WARNING: le nom de l'auteur.e '{name[1]} {name[0]}' "
            f"ne correspond pas au nom dans OpenAlex '{author['display_name']}'!{Colors.RESET}",
            soft_wrap=True,
        )

    # Check institutions & affiliations
    institution_match: bool = any(
        any(
            to_lower_no_accents_no_hyphens(local_affiliation["name"])
            in to_lower_no_accents_no_hyphens(institution["display_name"])
            for local_affiliation in reference_query.local_affiliations
        )
        for institution in author["last_known_institutions"]
    )
    affiliation_match: bool = any(
        any(
            to_lower_no_accents_no_hyphens(local_affiliation["name"])
            in to_lower_no_accents_no_hyphens(
                affiliation["institution"]["display_name"]
            )
            for local_affiliation in reference_query.local_affiliations
        )
        for affiliation in author["affiliations"]
    )
    if not institution_match and not affiliation_match:
        e = f"{e} Affl" if e else "Affl"
        if institutions := "; ".join(
            [
                institution["display_name"]
                for institution in author["last_known_institutions"]
            ]
        ):
            console.print(
                f"{Colors.YELLOW}WARNING - l'affiliation de l'auteur.e '{name[1]} {name[0]}' "
                f"est non-locale: '{institutions}'!{Colors.RESET}",
                soft_wrap=True,
            )
        elif affiliations := "; ".join(
            [
                affiliation["institution"]["display_name"]
                for affiliation in author["affiliations"]
            ]
        ):
            console.print(
                f"{Colors.YELLOW}WARNING - l'affiliation de l'auteur.e '{name[1]} {name[0]}' "
                f"est non-locale: '{affiliations}'!{Colors.RESET}",
                soft_wrap=True,
            )
        else:
            console.print(
                f"{Colors.YELLOW}WARNING - l'auteur.e '{name[1]} {name[0]}' "
                f"n'a pas d'affiliation dans OpenAlex!{Colors.RESET}",
                soft_wrap=True,
            )

    return e


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
        if openalex_id:
            try:
                author = Authors()[openalex_id]
                data_rows.append(
                    [
                        name[0],
                        name[1],
                        f'=HYPERLINK("https://openalex.org/{openalex_id}")',
                        f'=HYPERLINK("{author["orcid"]}")' if author["orcid"] else None,
                        _check_author_name_and_affiliation_correspondance(
                            reference_query=reference_query, author=author, name=name
                        ),
                        author["display_name"],
                        author["works_count"],
                        (
                            author["summary_stats"]["h_index"]
                            if "h_index" in author["summary_stats"]
                            else None
                        ),
                        (
                            [
                                institution["display_name"]
                                for institution in author["last_known_institutions"]
                            ]
                            if author["last_known_institutions"]
                            else None
                        ),
                        (
                            [
                                affiliation["institution"]["display_name"]
                                for affiliation in author["affiliations"]
                            ]
                            if author["affiliations"]
                            else None
                        ),
                    ]
                )
            except Exception as e:
                data_rows.append(
                    [
                        name[0],
                        name[1],
                        f"{openalex_id}",
                        None,
                        "ID",
                        None,
                        None,
                        None,
                        None,
                        None,
                    ]
                )
                console.print(
                    f"{Colors.RED}Erreur - l'identifiant OpenAlex {openalex_id} pour l'auteur "
                    f"'{name[1]} {name[0]}' est invalide ({e})!{Colors.RESET}",
                    soft_wrap=True,
                )
        else:
            data_rows.append(
                [
                    name[0],
                    name[1],
                    None,
                    None,
                    "ID",
                    None,
                    None,
                    None,
                    None,
                    None,
                ]
            )
            console.print(
                f"{Colors.RED}ERREUR - L'auteur.e '{name[1]} {name[0]}' n'a pas d'identifiant OpenAlex!{Colors.RESET}",
                soft_wrap=True,
            )

    author_profiles: pd.DataFrame = pd.DataFrame(
        data_rows,
        columns=[
            "Nom de famille",
            "Prénom",
            "Profil OpenAlex",
            "Profil ORCID",
            "Erreurs",
            "OpenAlex - display_name",
            "Nb publies",
            "H index",
            "Institutions",
            "Affiliations",
        ],
    )

    # Remove temporary columns
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

    def set_affiliation_and_id_column(row) -> str | None:
        affiliation_match: bool = any(
            (
                any(
                    local_affiliation["name"]
                    in to_lower_no_accents_no_hyphens(affiliation)
                    for affiliation in row["Affiliations"]
                )
                if row["Affiliations"]
                else False
            )
            for local_affiliation in reference_query.local_affiliations
        )
        if row["OpenAlex profile"]:
            match = re.search(r"A\d{10}", row["OpenAlex profile"])
            if match:
                au_id_index = reference_query.au_id_to_index.get(match.group())
            else:
                au_id_index = None
        else:
            au_id_index = None
        au_id_match: bool = au_id_index is not None and to_lower_no_accents_no_hyphens(
            reference_query.au_names[au_id_index][0]
        ) == to_lower_no_accents_no_hyphens(row["Surname"])
        if affiliation_match and au_id_match:
            return "Affl. + ID"
        elif affiliation_match:
            return "Affl."
        elif au_id_match:
            return "ID"
        else:
            return None

    # Add the "Affl/ID" column to the input dataframe
    reference_query.au_id_to_index = {
        au_id: index for index, au_id in enumerate(reference_query.openalex_ids)
    }
    author_profiles["Affl/ID"] = author_profiles.apply(  # type: ignore[call-overload]
        set_affiliation_and_id_column, axis=1
    )

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
                f"{Colors.RED}ERREUR - Aucun résultat dans OpenAlex pour {name[1]} {name[0]}!{Colors.RESET}"
            )
        data_rows.extend(
            [
                name[0],
                name[1],
                author["display_name"],
                f'=HYPERLINK("{author["id"]}")',
                f'=HYPERLINK("{author["orcid"]}")' if author["orcid"] else "",
                author["works_count"],
                (
                    [
                        last_inst["display_name"]
                        for last_inst in author["last_known_institutions"]
                    ]
                    if author["last_known_institutions"]
                    else None
                ),
                (
                    [
                        affiliation["institution"]["display_name"]
                        for affiliation in author["affiliations"]
                    ]
                    if author["affiliations"]
                    else None
                ),
                (
                    [topic["display_name"] for topic in author["topics"]]
                    if author["topics"]
                    else None
                ),
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
            "ORCID profile",
            "Pub count",
            "Institutions",
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
                            html.unescape(affiliation["name"])
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


def _add_local_author_name_and_count_columns_drop_duplicates(
    publications: pd.DataFrame,
) -> pd.DataFrame:

    # Columns to use for removing publication duplicates
    # match_criteria_columns: list[str] = ["title", "subtype", "doi"]
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

    # Add column of duplicate indices
    def find_duplicate_indices(row) -> list[int]:
        indices: list = [
            index
            for index, row_all in publications.iterrows()
            if all(
                row_all[criteria] == row[criteria]
                for criteria in match_criteria_columns
            )
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
        publications_without_duplicate.apply(count_local_coauthors, axis=1)  # type: ignore[call-overload]
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

    # Drop temporary columns
    publications_without_duplicate.drop(
        ["duplicates", "Duplicate indices"], axis=1, inplace=True
    )

    return publications_without_duplicate


def _consolidate_subtypes(work_type: str, publication_name: str | None) -> str:
    """
    Constrain subtypes to the set specified by the publication_types_openalex parameter
    in the input .toml file, put HAL publications in separate catégory and their subtypes
    are inconsistent.

    Args:
        work_type (str): publications type to consolidate, if not in input set
        publication_name (str): publications name

    Returns : consolidated subtype
    """

    subtypes = {
        "article": "preprint",
        "journal-article": "journal-article",
        "proceedings-article": "proceedings-article",
        "book-chapter": "book-chapter",
        "preprint": "preprint",
        "posted-content": "preprint",
        "journal-preprint": "preprint",
        "other": "other",
    }

    if publication_name is not None and "HAL" in publication_name:
        return "HAL"
    if work_type in subtypes:
        return subtypes[work_type]
    console.print(
        f"{Colors.YELLOW}WARNING: subtype '{work_type}' inconnu dans la recherche de publications, "
        "ajouter ce subtype à la fonction search_openalex._consolidate_subtypes()!{Colors.RESET}",
        soft_wrap=True,
    )
    return "other"


def _check_3it_affiliation(authorships: list) -> bool:
    return any(
        any(
            (
                "institut" in html.unescape(raw_affiliation_string).lower()
                and "interdisciplinaire"
                in html.unescape(raw_affiliation_string).lower()
                and "innovation" in html.unescape(raw_affiliation_string).lower()
                and "technologique" in html.unescape(raw_affiliation_string).lower()
                for raw_affiliation_string in author["raw_affiliation_strings"]
            )
            or (
                "interdisciplinary" in html.unescape(raw_affiliation_string).lower()
                and "institute" in html.unescape(raw_affiliation_string).lower()
                and "technological" in html.unescape(raw_affiliation_string).lower()
                and "innovation" in html.unescape(raw_affiliation_string).lower()
                for raw_affiliation_string in author["raw_affiliation_strings"]
            )
        )
        for author in authorships
    )


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
        works_df = pd.DataFrame([])
        if openalex_id:
            date_range = {
                "from_publication_date": reference_query.date_start.strftime(
                    "%Y-%m-%d"
                ),
                "to_publication_date": reference_query.date_end.strftime("%Y-%m-%d"),
            }
            works = Works().filter(author={"id": openalex_id}, **date_range)
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
                    authors_crossref = publication_info_from_crossref["authors"]
                    author_affiliations_crossref = publication_info_from_crossref[
                        "Affiliations"
                    ]
                    type_crossref = publication_info_from_crossref["type"]
                    publication_name_crossref = publication_info_from_crossref[
                        "publication_name"
                    ]
                    volume = publication_info_from_crossref["volume"]
                else:
                    authors_crossref = None
                    author_affiliations_crossref = None
                    type_crossref = None
                    publication_name_crossref = None
                    volume = None

                # Store record if the publication name is available either in the OpenAlex or Crossref records
                if (
                    publication_name_openalex is not None
                    or publication_name_crossref is not None
                ):
                    # Consolidate OpenAlex & Crossref record fields
                    work_type: str = type_crossref or (type_openalex or "other")
                    work_title: str = title_openalex or title_openalex
                    work_publication_name: str | None = (
                        publication_name_crossref or publication_name_openalex
                    )
                    if len(authors_crossref) > len(authors_openalex):
                        authors = authors_crossref
                        affiliations = author_affiliations_crossref
                    else:
                        authors = authors_openalex
                        affiliations = author_affiliations_openalex

                    # Consolidate subtypes to the set specified by the user (HAL category, check for article in openalex vs preprint in crossref)
                    work_type = _consolidate_subtypes(
                        work_type=work_type, publication_name=work_publication_name
                    )

                    # Special case for HAL open archive entries (https://hal.science/)
                    if work_type == "HAL":
                        work_publication_name = f"HAL ({work['primary_location']['raw_source_name'] if 'raw_source_name' in work['primary_location'] else 'HAL'})"

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
                                    "Affiliation 3IT": [
                                        (
                                            "X"
                                            if _check_3it_affiliation(
                                                work["authorships"]
                                            )
                                            else None
                                        )
                                    ],
                                    "author_names": [authors],
                                    "institutions": [author_institutions_openalex],
                                    "affiliations": [affiliations],
                                    "publicationName": [work_publication_name],
                                    "volume": [volume],
                                    "doi": [f'=HYPERLINK("{work["doi"]}")'],
                                    "id": [f'=HYPERLINK("{work["id"]}")'],
                                }
                            ),
                        ],
                        ignore_index=True,
                    )

        # Add the dataframe for this author to the dataframe of all publications
        if not works_df.empty:
            publications = pd.concat([publications, works_df])

        # Update the author publications counts by type
        pub_type_counts_by_author.append(
            count_publications_by_type_in_df(
                publication_type_codes=reference_query.publication_type_codes,
                df=works_df,
            )
        )

    # Check for no publications found!
    if publications.empty:
        console.print(
            f"{Colors.RED}ERREUR - aucune publication trouvée dans OpenAlex pour la période du "
            f"{reference_query.date_start} au {reference_query.date_end}!{Colors.RESET}",
            soft_wrap=True,
        )
    else:
        # Remove duplicates and add local author name and count columns
        publications.reset_index(drop=True, inplace=True)
        publications = _add_local_author_name_and_count_columns_drop_duplicates(
            publications=publications
        )

        # Sort by title (date is unreliable)
        publications = publications.sort_values(by=["title"])
        publications.reset_index(drop=True, inplace=True)

    # Reformat pub_type_counts_by_author list
    pub_type_counts_by_author_transpose: list = [
        list(row) for row in zip(*pub_type_counts_by_author)
    ]

    return publications, pub_type_counts_by_author_transpose
