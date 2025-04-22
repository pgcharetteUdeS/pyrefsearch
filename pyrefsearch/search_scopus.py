"""services/search_scopus.py

    Search Scopus database for author profiles and publications

    The script uses the "pybliometrics" for Scopus searches,
    see https://pybliometrics.readthedocs.io/en/stable/
    NB: An API key is required to query the Scopus API,
        see https://dev.elsevier.com/index.jsp. The first execution of the script
        will prompt the user to enter the key.

"""

__all__ = [
    "scopus_init_api",
    "query_scopus_author_profiles",
    "query_scopus_author_profiles_by_id",
    "query_scopus_author_profiles_by_name",
    "query_scopus_publications",
]

import pandas as pd
import pybliometrics
from pybliometrics.exception import ScopusException
from pybliometrics.scopus import AuthorRetrieval, AuthorSearch, ScopusSearch
import sys

from referencequery import ReferenceQuery
from utils import console, to_lower_no_accents_no_hyphens


def _check_author_name_correspondance(
    reference_query: ReferenceQuery, authors: pd.DataFrame
) -> list:
    """

    Check that the author names & affiliations supplied in the input Excel file
    correspond to the information associated with their IDs in the Scopus database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        authors (pd.DataFrame): DataFrame with author profiles

    Returns: List of errors per author

    """

    # Loop through authors to check for discrepancies/errors
    query_errors: list[str | None] = []
    for i, [
        [input_last_name, input_first_name],
        au_id,
        scopus_last_name,
        scopus_first_name,
        affiliation,
        parent_affiliation,
    ] in enumerate(
        zip(
            reference_query.au_names,
            reference_query.au_ids,
            authors["Nom de famille"],
            authors["Prénom"],
            authors["Affiliation"],
            authors["Affiliation mère"],
        )
    ):
        query_error: str | None = None
        if scopus_last_name is None:
            # Missing Scopus ID, enter name manually into authors profile dataframe
            authors.loc[i, "Nom de famille"] = input_last_name
            authors.loc[i, "Prénom"] = input_first_name
            query_error = "Aucun identifiant Scopus"
            console.print(
                f"[yellow]WARNING: L'auteur.e '{input_last_name}, {input_first_name}' "
                "n'a pas d'identifiant Scopus[/yellow]",
                soft_wrap=True,
            )
        else:
            # Check for name discrepancies between input and Scopus database
            if to_lower_no_accents_no_hyphens(
                scopus_last_name
            ) != to_lower_no_accents_no_hyphens(input_last_name):
                query_error = "Disparité de noms de famille"
                console.print(
                    f"[red]ERREUR pour l'identifiant {au_id}: "
                    f"le nom de famille de l'auteur.e '{input_last_name}, "
                    f"{input_first_name}' dans {reference_query.in_excel_file} diffère"
                    f" de '{scopus_last_name}, {scopus_first_name}'"
                    " dans la base de données Scopus![/red]",
                    soft_wrap=True,
                )

            # Check for affiliation discrepancies between input and Scopus database
            affiliation_tl: str = (
                ""
                if affiliation is None
                else to_lower_no_accents_no_hyphens(affiliation)
            )
            parent_affiliation_tl: str = (
                ""
                if parent_affiliation is None
                else to_lower_no_accents_no_hyphens(parent_affiliation)
            )
            if all(
                s not in affiliation_tl and s not in parent_affiliation_tl
                for s in reference_query.local_affiliations
            ):
                query_error = (
                    "Affiliation non locale"
                    if query_error is None
                    else "Disparité de noms de famille / Affiliation non locale"
                )
                console.print(
                    f"[red]ERREUR pour l'identifiant {au_id} "
                    f"({input_last_name}, {input_first_name}): "
                    f"l'affiliation '{affiliation}, {parent_affiliation}' "
                    "est non locale![/red]",
                    soft_wrap=True,
                )

        # Append current error to author error list
        query_errors.append(query_error)

    return query_errors


def _count_publications_by_type_in_df(
    reference_query: ReferenceQuery, df: pd.DataFrame
) -> list:
    """
    Count number of publications by type in a dataframe

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        df (pd.DataFrame): DataFrame with publications

    Returns: List of counts per publication type

    """

    if df.empty:
        return [None] * len(reference_query.publication_type_codes)
    else:
        return [
            (
                len(df[df["subtype"] == pub_type])
                if len(df[df["subtype"] == pub_type]) > 0
                else None
            )
            for pub_type in reference_query.publication_type_codes
        ]


def _add_coauthor_columns_and_clean_up_publications_df(
    publications_in: pd.DataFrame, reference_query: ReferenceQuery
) -> pd.DataFrame:
    """
    Add columns listing names and counts of local coauthors to the publications DataFrame,
    and sort by publication date

    Args:
        publications_in (pd.DataFrame): DataFrame with publications
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: DataFrame with added columns and sorted by publication date
    """

    # Remove duplicates
    publications: pd.DataFrame = publications_in.drop_duplicates("eid").copy()

    # Add columns listing names and counts of local coauthors
    def find_local_coauthors(author_ids) -> list:
        co_authors_local: list[str] = [
            name[0]
            for name, au_id in zip(reference_query.au_names, reference_query.au_ids)
            if any([str(au_id) in author_ids]) and au_id > 0
        ]
        return co_authors_local

    publications["Auteurs locaux"] = publications["author_ids"].apply(
        find_local_coauthors
    )
    publications["Nb co-auteurs locaux"] = [
        len(co_authors) if len(co_authors) > 1 else None
        for co_authors in publications["Auteurs locaux"]
    ]

    # Check that there is at least one local author in the list of author Scopus IDs.
    # If not, the only local author probably has more than one Scopus ID, show warning.
    for _, row in publications.iterrows():
        if not row["Auteurs locaux"]:
            console.print(
                f"[yellow]WARNING: Le document '{row['title']}' "
                f"({row['subtypeDescription']}) n'a pas "
                "d'ID scopus local dans les auteurs.[/yellow]",
                end=" ",
                soft_wrap=True,
            )
            problem_author: str = ""
            for author in reference_query.au_names:
                if author[0] in row["author_names"]:
                    problem_author = author[0]
                    break
            if problem_author:
                console.print(
                    f"[yellow]Cause probable : l'auteur '{problem_author}' "
                    "a plus d'un ID Scopus.[/yellow]",
                    soft_wrap=True,
                )
            else:
                console.print("", soft_wrap=True)

    # Sort by publication date
    publications = publications.sort_values(by=["coverDate"])

    return publications


def _flag_matched_scopus_author_ids_and_affiliations(
    reference_query: ReferenceQuery, author_profiles: pd.DataFrame
) -> pd.DataFrame:
    """
    Flag author profiles with local affiliations and matching Scopus IDs between
    input Excel file and Scopus database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        author_profiles (pd.DataFrame): DataFrame with author profiles

    Returns: DataFrame with local author profiles flagged

    """

    def set_affiliation_and_id(row) -> str | None:
        if row.affiliation is None:
            return None

        local_affiliation_match: bool = any(
            s in to_lower_no_accents_no_hyphens(row.affiliation)
            for s in reference_query.local_affiliations
        )
        au_id_index: int | None = reference_query.au_id_to_index.get(int(row.eid))
        au_id_match: bool = au_id_index is not None and to_lower_no_accents_no_hyphens(
            reference_query.au_names[au_id_index][0]
        ) == to_lower_no_accents_no_hyphens(row.surname)
        if local_affiliation_match and au_id_match:
            return "Affl. + ID"
        elif local_affiliation_match:
            return "Affl."
        elif au_id_match:
            return "ID"
        else:
            return None

    # Precompute dictionary mapping Scopus IDs to their indices for constant-time lookups.
    reference_query.au_id_to_index = {
        au_id: index for index, au_id in enumerate(reference_query.au_ids)
    }
    author_profiles["Affl/ID"] = author_profiles.apply(set_affiliation_and_id, axis=1)

    return author_profiles


def _reindex_author_profiles_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reindex and re-order columns in author profiles DataFrame
    (the Scopus database indexing puts the names far down the list)

    Args:
        df (pd.DataFrame): DataFrame with author profiles

    Returns: DataFrame with re-indexed & re-ordered author profiles

    """

    df.reset_index(drop=True, inplace=True)
    df = df[
        pd.Index(
            [
                "surname",
                "givenname",
                "initials",
                "Affl/ID",
                "Start",
                "End",
                "eid",
                "affiliation",
                "affiliation_id",
                "country",
                "city",
                "orcid",
                "areas",
                "documents",
            ]
        )
    ]
    return df


def query_scopus_author_profiles_by_id(reference_query: ReferenceQuery) -> pd.DataFrame:
    """

    Fetch author profiles from their IDs in the Scopus database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object

    Returns: DataFrame with author profiles

    """

    author_profiles = []
    columns: list[str] = [
        "Nom de famille",
        "Prénom",
        "ID Scopus",
        "Affiliation",
        "Affiliation mère",
        "Période active",
    ]
    for i, [name, au_id] in enumerate(
        zip(reference_query.au_names, reference_query.au_ids)
    ):
        try:
            if au_id > 0:
                author = AuthorRetrieval(
                    author_id=au_id,
                    refresh=reference_query.scopus_database_refresh_days,
                )
                author_profiles.append(
                    [
                        author.surname,
                        author.given_name,
                        au_id,
                        author.affiliation_current[0].__getattribute__(
                            "preferred_name"
                        ),
                        author.affiliation_current[0].__getattribute__(
                            "parent_preferred_name"
                        ),
                        author.publication_range,
                    ]
                )
            else:
                author_profiles.append([None] * len(columns))
        except ScopusException as e:
            vpn_required_str: str = (
                " ou tentative d'accès hors du réseau "
                "universitaire UdeS (VPN requis)"
                if i == 0
                else ""
            )
            console.print(
                f"[red]Erreur dans la recherche Scopus à la ligne {i + 2} "
                f"({name[0]}, {name[1]}) "
                f"du fichier {reference_query.in_excel_file}  - '{e}' - "
                f"Causes possibles: identifiant Scopus inconnu{vpn_required_str}![/red]",
                soft_wrap=True,
            )
            sys.exit()

    # Create author profiles DataFrame, flag discrepancies between input and Scopus data
    author_profiles_by_ids: pd.DataFrame = pd.DataFrame()
    if author_profiles:
        author_profiles_by_ids = pd.DataFrame(author_profiles, columns=columns)
        author_profiles_by_ids.insert(
            loc=3,
            column="Erreurs",
            value=pd.Series(
                _check_author_name_correspondance(
                    reference_query=reference_query, authors=author_profiles_by_ids
                )
            ),
        )

    return author_profiles_by_ids


def query_scopus_author_profiles_by_name(
    reference_query: ReferenceQuery,
    homonyms_only: bool = True,
) -> pd.DataFrame:
    """
    Fetch author profiles by name in the Scopus database. If homonyms_only is True,
    retain only author profiles with homonyms (multiple Scopus IDs for same name).

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        homonyms_only (bool): Include only author profiles with homonyms

    Returns: DataFrame with author search results

    """

    author_profiles_all = pd.DataFrame()
    for name in reference_query.au_names:
        query_string: str = f"AUTHLAST({name[0]}) and AUTHFIRST({name[1]})"
        author_profiles_from_name_search_results = AuthorSearch(
            query=query_string,
            refresh=reference_query.scopus_database_refresh_days,
            verbose=True,
        )
        if author_profiles_from_name_search_results.authors:
            author_profiles_from_name = pd.DataFrame(
                author_profiles_from_name_search_results.authors
            )
            author_profiles_from_name["eid"] = [
                au_id.split("-")[-1]
                for au_id in author_profiles_from_name.eid.to_list()
            ]
            (
                author_profiles_from_name["Start"],
                author_profiles_from_name["End"],
            ) = zip(
                *[
                    AuthorRetrieval(
                        author_id=au_id,
                        refresh=reference_query.scopus_database_refresh_days,
                    ).publication_range
                    for au_id in author_profiles_from_name.eid.to_list()
                ]
            )
            if not homonyms_only or author_profiles_from_name.shape[0] > 1:
                author_profiles_all = pd.concat(
                    [author_profiles_all, author_profiles_from_name],
                    ignore_index=True,
                )
                author_profiles_all.loc[len(author_profiles_all)] = [None] * len(
                    author_profiles_all.columns
                )
        elif not homonyms_only:
            console.print(
                f"[red]ERREUR: aucun résultat pour l'auteur.e '{name[0]}, {name[1]}' [/red]",
                soft_wrap=True,
            )

    if not author_profiles_all.empty:
        author_profiles_all = _flag_matched_scopus_author_ids_and_affiliations(
            reference_query=reference_query, author_profiles=author_profiles_all
        )
        author_profiles_all = _reindex_author_profiles_df(df=author_profiles_all)

    return author_profiles_all


def query_scopus_publications(
    reference_query: ReferenceQuery,
) -> tuple[pd.DataFrame, list[list[int | None]]]:
    """
    Fetch publications for range of years in Scopus database for list of author IDs

    Scopus document type search terms:
      Article-ar / Abstract Report-ab / Book-bk / Book Chapter-ch / Conference Paper-cp /
      Conference Review-cr / Data Paper-dp / Editorial-ed / Erratum-er / Letter-le /
      Multimedia-mm / Note-no / Report-rp / Retracted-tb / Review-re / Short Survey-sh

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns : DataFrame with publication search results (pd.DataFrame),
              list of publication type counts by author (list)
    """

    # Build Scopus document type search string
    pub_types_search_string: str = " OR ".join(
        [f"DOCTYPE ({s})" for s in reference_query.publication_type_codes]
    )

    # Loop through list of author IDs to fetch publications, count pub types by author
    publications = pd.DataFrame()
    pub_type_counts_by_author: list = []
    for au_id in reference_query.au_ids:
        if au_id > 0:
            query_str: str = (
                f"AU-ID ({au_id})"
                f" AND PUBYEAR > {reference_query.pub_year_first - 1}"
                f" AND PUBYEAR < {reference_query.pub_year_last + 1}"
                f" AND ({pub_types_search_string})"
            )
            try:
                query_results = ScopusSearch(
                    query=query_str,
                    refresh=reference_query.scopus_database_refresh_days,
                    verbose=True,
                )
            except ScopusException as e:
                console.print(
                    f"[red]Erreur dans la recherche Scopus pour l'identifiant {au_id}, "
                    f"causes possibles: identifiant inconnu ou tentative d'accès "
                    f"hors du réseau universitaire UdeS (VPN requis) - '{e}'![/red]",
                    soft_wrap=True,
                )
                sys.exit()

            author_pubs = pd.DataFrame(query_results.results)
            pub_type_counts_by_author.append(
                _count_publications_by_type_in_df(
                    reference_query=reference_query, df=author_pubs
                )
            )
            publications = pd.concat([publications, author_pubs])
        else:
            pub_type_counts_by_author.append(
                [None] * len(reference_query.publication_type_codes)
            )
    pub_type_counts_by_author_transpose: list = [
        list(row) for row in zip(*pub_type_counts_by_author)
    ]

    if not publications.empty:
        publications = _add_coauthor_columns_and_clean_up_publications_df(
            publications, reference_query
        )

    return publications, pub_type_counts_by_author_transpose


def query_scopus_author_profiles(reference_query: ReferenceQuery) -> None:
    """
    Query Scopus for a list of author profiles by name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: None

    """

    console.print(
        "Recherche de profils d'auteur.e.s par nom dans la base de données Scopus",
        soft_wrap=True,
    )

    # Init Scopus API
    scopus_init_api()

    author_profiles_by_name: pd.DataFrame = query_scopus_author_profiles_by_name(
        reference_query=reference_query,
        homonyms_only=False,
    )
    author_profiles_by_name.rename(
        columns={
            "eid": "ID Scopus",
        },
        inplace=True,
    )
    with pd.ExcelWriter(reference_query.out_excel_file) as writer:
        author_profiles_by_name.to_excel(writer, index=False, sheet_name="Profils")
    console.print(
        "Résultats de la recherche sauvegardés "
        f"dans le fichier '{reference_query.out_excel_file}'",
        soft_wrap=True,
    )


def scopus_init_api() -> None:
    """
    Initialize Scopus API

    Args: None

    Returns: None

    """

    pybliometrics.scopus.init()
