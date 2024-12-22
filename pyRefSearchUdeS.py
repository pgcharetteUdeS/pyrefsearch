"""pyRefSearchUdeS.py

    For a list of author names and range of years supplied in an input Excel file,
    query references (publications in Scopus, patents in the USPTO database) OR
    author profiles (Scopus database), and write the results to an output Excel file.

    The script uses the "pybliometrics" for Scopus searches,
    see https://pybliometrics.readthedocs.io/en/stable/

    NB: A "key" is required to query the Scopus API, see https://dev.elsevier.com/index.jsp.
    The first execution of the script will prompt the user to enter the key.

    The script uses the "patent_client" package for searches in the USPTO database,
    see https://patent-client.readthedocs.io/en/latest/user_guide/fulltext.html

    Project on gitHub: https://github.com/pgcharetteUdeS/pyRefSearchUdeS

"""

import pybliometrics
import pandas as pd
from patent_client import Patent, PublishedApplication
from pathlib import Path
from pybliometrics.scopus import AuthorRetrieval, AuthorSearch, ScopusSearch
from pybliometrics.scopus.exception import ScopusException
import re
from rich import print
import sys
import toml
import unidecode
from version import __version__
import warnings


class ReferenceQuery:
    """
    Class to store reference query parameters
    """

    @staticmethod
    def check_excel_file_access(filename: Path):
        try:
            with open(filename, "a"):
                pass
        except IOError:
            raise IOError(
                f"Could not open '{filename}', close it " "if it's already open!"
            ) from None

    def __init__(
        self,
        in_excel_file: Path,
        in_excel_file_author_sheet: str,
        out_excel_file: Path,
        pub_year_first: int,
        pub_year_last: int,
        publication_types: list[str],
        local_affiliations: list[str],
        scopus_database_refresh: bool | int,
    ):
        self.in_excel_file: Path = in_excel_file
        self.out_excel_file: Path = out_excel_file
        self.pub_year_first: int = pub_year_first
        self.pub_year_last: int = pub_year_last
        self.publication_types: list[str] = [row[0] for row in publication_types]
        self.publication_type_codes: list[str] = [row[1] for row in publication_types]
        self.local_affiliations: list[str] = [
            _to_lower_no_accents_no_hyphens(s) for s in local_affiliations
        ]
        self.scopus_database_refresh: bool | int = scopus_database_refresh

        # Check input/output Excel file access, script fails if files already open
        self.check_excel_file_access(self.in_excel_file)
        self.check_excel_file_access(self.out_excel_file)

        # Load input Excel file into a dataframe, remove rows without author names
        warnings.simplefilter(action="ignore", category=UserWarning)
        input_data_full = pd.read_excel(
            self.in_excel_file, sheet_name=in_excel_file_author_sheet
        )
        input_data_full.dropna(subset=["Nom"], inplace=True)

        # Extract author names from input Excel file
        author_status_by_year_columns = [
            f"{year}-{year+1}"
            for year in range(self.pub_year_first, self.pub_year_last + 1)
        ]
        if all(col in input_data_full.columns for col in author_status_by_year_columns):
            # Author information is tabulated by fiscal year (XXXX-YYYY) and status (full
            # member or collaborator). Validate that the range of years specified
            # in the input data covers the range of years specified in the query,
            # filter by member status/year to remove collaborators.
            authors = input_data_full.copy()[
                ["Nom", "Prénom", "ID Scopus"] + author_status_by_year_columns
            ]
            authors["status"] = [
                "Régulier" if "Régulier" in yearly_status else "Collaborateur"
                for yearly_status in authors[
                    author_status_by_year_columns
                ].values.tolist()
            ]
            authors.drop(authors[authors.status == "Collaborateur"].index, inplace=True)

        elif not any(
            # Author information is supplied as a simple list of names, no filtering
            re.search(r"\d{4}-\d{4}", column)
            for column in input_data_full.columns.tolist()
        ):
            authors = input_data_full.copy()[["Nom", "Prénom", "ID Scopus"]]

        else:
            raise IOError(
                f"Range of years [{self.pub_year_first}-{self.pub_year_last}] exceeds "
                f"the available data in '{in_excel_file}'!"
            ) from None
        self.au_names = authors[["Nom", "Prénom"]].values.tolist()

        # Extract Scopus IDs, replace non-integer values with 0
        self.au_ids = []
        if "ID Scopus" in authors:
            for scopus_id in authors["ID Scopus"].values.tolist():
                try:
                    self.au_ids.append(int(scopus_id))
                except ValueError:
                    self.au_ids.append(0)


def _to_lower_no_accents_no_hyphens(s: str | pd.Series) -> str:
    """
    Convert string to lower case and remove accents and hyphens

    Args:
        s (str): Input string

    Returns: String in lower case without accents

    """

    return unidecode.unidecode(s.replace("-", " ").lower().strip())


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
                "Affl/ID",
                "surname",
                "givenname",
                "initials",
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


def _flag_matched_scopus_author_ids_and_affiliations(
    reference_query: ReferenceQuery, author_profiles_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Flag author profiles with local affiliations and matching Scopus IDs between
    input Excel file and Scopus database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        author_profiles_df (pd.DataFrame): DataFrame with author profiles

    Returns: DataFrame with local author profiles flagged

    """

    local_column: list = [None] * len(author_profiles_df)
    for i, row in author_profiles_df.iterrows():
        if row.affiliation is not None:
            local_affiliation_match: bool = any(
                s in _to_lower_no_accents_no_hyphens(row.affiliation)
                for s in reference_query.local_affiliations
            )
            au_id_index: int | None = (
                reference_query.au_ids.index(int(row.eid))
                if int(row.eid) in reference_query.au_ids
                else None
            )
            au_id_match: bool = (
                au_id_index is not None
                and _to_lower_no_accents_no_hyphens(
                    reference_query.au_names[au_id_index][0]
                )
                == _to_lower_no_accents_no_hyphens(row.surname)
            )
            if local_affiliation_match and au_id_match:
                local_column[hash(i)] = "Affl. + ID"
            elif local_affiliation_match:
                local_column[hash(i)] = "Affl."
            elif au_id_match:
                local_column[hash(i)] = "ID"
    author_profiles_df["Affl/ID"] = local_column

    return author_profiles_df


def _check_author_name_correspondance(
    reference_query: ReferenceQuery, authors_df: pd.DataFrame
) -> list:
    """

    Check that the author names supplied in the input Excel file correspond to the
    author names associated with their IDs in the Scopus database, and local
    affiliations

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        authors_df (pd.DataFrame): DataFrame with author profiles

    Returns: List of errors per author

    """

    query_errors: list = [None] * len(reference_query.au_ids)
    for i, [input_last_name, input_first_name] in enumerate(reference_query.au_names):
        # Check for missing Scopus ID
        if authors_df["Nom de famille"][i] is None:
            authors_df.loc[i, "Nom de famille"] = input_last_name
            authors_df.loc[i, "Prénom"] = input_first_name
            query_errors[i] = "Aucun identifiant Scopus"
            print(
                f"[yellow]WARNING: L'auteur.e '{input_last_name}, {input_first_name}' "
                "n'a pas d'identifiant Scopus[/yellow]"
            )
        else:
            # Check for name discrepancies
            scopus_last_name: str = _to_lower_no_accents_no_hyphens(
                authors_df["Nom de famille"][i]
            )
            if scopus_last_name != _to_lower_no_accents_no_hyphens(input_last_name):
                query_errors[i] = "Disparité de noms de famille"
                print(
                    f"[red]ERREUR pour l'identifiant {reference_query.au_ids[i]}: "
                    f"le nom de famille de l'auteur.e '{input_last_name}, "
                    f"{input_first_name}' dans {reference_query.in_excel_file} diffère de "
                    f"'{authors_df['Nom de famille'][i]}, {authors_df['Prénom'][i]}'"
                    " dans la base de données Scopus![/red]"
                )

            # Check for local affiliation discrepancies
            affiliation: str = (
                ""
                if authors_df["Affiliation"][i] is None
                else _to_lower_no_accents_no_hyphens(authors_df["Affiliation"][i])
            )
            parent_affiliation: str = (
                ""
                if authors_df["Affiliation mère"][i] is None
                else _to_lower_no_accents_no_hyphens(authors_df["Affiliation mère"][i])
            )
            if all(
                s not in affiliation and s not in parent_affiliation
                for s in reference_query.local_affiliations
            ):
                query_errors[i] = (
                    "Affiliation non locale"
                    if query_errors[i] is None
                    else "Disparité de noms de famille / Affiliation non locale"
                )
                print(
                    f"[red]ERREUR pour l'identifiant {reference_query.au_ids[i]} "
                    f"({input_last_name}, {input_first_name}): "
                    f"l'affiliation '{affiliation}, {parent_affiliation}' "
                    "est non locale![/red]"
                )

    return query_errors


def _count_publications_by_type_in_df(
    reference_query: ReferenceQuery, df: pd.DataFrame
) -> list:
    """
    Count number of publications by type in a dataframe

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        df (pd.DataFrame): DataFrame with publications

    Returns: List of counts

    """

    pub_type_counts: list = [None] * len(reference_query.publication_type_codes)
    if not df.empty:
        for i, pub_type in enumerate(reference_query.publication_type_codes):
            if len(df[df["subtype"] == pub_type]) > 0:
                pub_type_counts[i] = len(df[df["subtype"] == pub_type])
    return pub_type_counts


def _build_patent_query_string(reference_query: ReferenceQuery, field_code: str) -> str:
    query_str = f'@{field_code}>="{reference_query.pub_year_first}0101"<="{reference_query.pub_year_last}1231" AND ('
    for i, name in enumerate(reference_query.au_names):
        if i > 0:
            query_str += " OR "
        query_str += f"(({name[1]} NEAR2 {name[0]}).IN.)"
    query_str += ")"
    return query_str


def _export_publications_df_to_excel_sheet(
    writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str
) -> None:
    """
    Write selected set of publication dataframe columns to Excel sheet

    Args:
        writer (pd.ExcelWriter): openpyxl writer object
        df (pd.DataFrame): articles dataframe
        sheet_name (str): Excel file sheet name

    Returns: None

    """

    if not df.empty:
        df.rename(
            columns={
                "coverDate": "Date",
                "title": "Titre",
                "Nb co-auteurs": "Nb co-auteurs",
                "Auteurs locaux": "Auteurs locaux",
                "author_names": "Auteurs",
                "publicationName": "Publication",
                "volume": "Volume",
                "pageRange": "Pages",
                "doi": "DOI",
            },
            inplace=True,
        )
        df[
            [
                "Date",
                "Titre",
                "Nb co-auteurs",
                "Auteurs locaux",
                "Auteurs",
                "Publication",
                "Volume",
                "Pages",
                "DOI",
            ]
        ].to_excel(writer, index=False, sheet_name=sheet_name)


def _tabulate_patents_per_author(
    reference_query: ReferenceQuery,
    patent_applications: pd.DataFrame,
    patents: pd.DataFrame,
) -> tuple[list, int, list, int]:
    """
    Count number of patents & applications for each author and number of joint patents

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        patent_applications (pd.DataFrame): patent application search results
        patents (pd.DataFrame): patent search results

    Returns: Number of patent applications per author (list),
             number of joint patent applications (int),
             number of patents per author (list),
             number of joint patents (int)

    """

    def _tabulate(docs: pd.DataFrame) -> tuple[list, int]:
        """
        Count number of patents or applications for each author and joint patents

        Args:
            docs (pd.DataFrame): patents or patent application search results

        Returns : Number of patents per author (list), number of joint patents (int)

        """

        author_docs_counts: list[int | None] = []
        joint_docs: int = 0
        if not docs.empty:
            # Count patents/applications per author
            for [lastname, firstname] in reference_query.au_names:
                author_docs_counts.append(
                    sum(
                        any(
                            (
                                _to_lower_no_accents_no_hyphens(lastname)
                                in _to_lower_no_accents_no_hyphens(inventor)
                                and _to_lower_no_accents_no_hyphens(firstname)
                                in _to_lower_no_accents_no_hyphens(inventor)
                                for inventor in row["Inventeurs"]
                            )
                        )
                        for _, row in docs.iterrows()
                    )
                )
                if author_docs_counts[-1] == 0:
                    author_docs_counts[-1] = None

            # Count joint patents/applications
            for _, row in docs.iterrows():
                if row["Nb co-inventeurs"] is not None and row["Nb co-inventeurs"] > 1:
                    joint_docs += 1

        return author_docs_counts, joint_docs

    joint_patent_applications_by_author: list[int] = []
    joint_patent_applications_count: int = 0
    joint_patents_by_author: list[int] = []
    joint_patents_count: int = 0

    if not patent_applications.empty:
        (
            joint_patent_applications_by_author,
            joint_patent_applications_count,
        ) = _tabulate(docs=patent_applications)
    if not patents.empty:
        joint_patents_by_author, joint_patents_count = _tabulate(docs=patents)

    return (
        joint_patent_applications_by_author,
        joint_patent_applications_count,
        joint_patents_by_author,
        joint_patents_count,
    )


def _add_coauthor_columns_and_clean_up_publications(
    publications_df_in: pd.DataFrame, reference_query: ReferenceQuery
) -> pd.DataFrame:
    """
    Add columns listing names and counts of local coauthors to the publications DataFrame,
    and sort by publication date

    Args:
        publications_df_in (pd.DataFrame): DataFrame with publications
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: DataFrame with added columns and sorted by publication date
    """

    # Remove duplicates
    publications_df = publications_df_in.drop_duplicates("eid").copy()

    # Add columns listing names and counts of local coauthors
    local_coauthors: list = []
    local_coauthors_counts: list = []
    for _, row in publications_df.iterrows():
        co_authors_local: list = [
            reference_query.au_names[i][0]
            for i, local_author_id in enumerate(reference_query.au_ids)
            if any([str(local_author_id) in row["author_ids"]]) and local_author_id > 0
        ]
        local_coauthors.append(co_authors_local)
        local_coauthors_counts.append(len(co_authors_local))
    publications_df["Auteurs locaux"] = local_coauthors
    publications_df["Nb co-auteurs"] = local_coauthors_counts

    # Sort by publication date
    publications_df.sort_values(by=["coverDate"], inplace=True)

    return publications_df


def _create_results_summary_df(
    reference_query: ReferenceQuery,
    publications_dfs_list_by_pub_type: list,
    patent_applications: pd.DataFrame,
    joint_patent_applications_count: int,
    patents: pd.DataFrame,
    joint_patents_count: int,
) -> pd.DataFrame:
    """
    Create results summary dataframe

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        publications_dfs_list_by_pub_type (list): list of DataFrames with search results by type
        patent_applications (pd.DataFrame): patent application search results
        joint_patent_applications_count (int): number of joint patent applications
        patents (pd.DataFrame): patent search results
        joint_patents_count (int): number of joint patents

    Returns: DataFrame with results summary

    """
    # Create results summary dataframe

    results: list = [
        None,
        "Nb d'auteur.e.s",
        "Année de début",
        "Année de fin",
    ]
    results += reference_query.publication_types
    results += ["Brevets US (en instance)", "Brevets US (délivrés)"]
    values: list = [
        None,
        len(reference_query.au_ids),
        reference_query.pub_year_first,
        reference_query.pub_year_last,
    ]
    if publications_dfs_list_by_pub_type:
        values += [0 if df.empty else len(df) for df in publications_dfs_list_by_pub_type]
    else:
        values += [None] * len(reference_query.publication_types)
    values += [
        len(patent_applications),
        len(patents),
    ]
    co_authors: list = ["Conjointes", None, None, None]
    if publications_dfs_list_by_pub_type:
        co_authors += [
            None if df.empty else len(df[df["Nb co-auteurs"] > 1])
            for df in publications_dfs_list_by_pub_type
        ]
    else:
        co_authors += [None] * len(reference_query.publication_types)
    co_authors += [joint_patent_applications_count, joint_patents_count]

    return pd.DataFrame([results, values, co_authors]).T


def write_reference_query_results_to_excel(
    reference_query: ReferenceQuery,
    publications_dfs_list_by_pub_type: list[pd.DataFrame],
    patents: pd.DataFrame,
    patent_applications: pd.DataFrame,
    author_profiles_by_ids_df: pd.DataFrame,
    author_profiles_by_names_df: pd.DataFrame,
) -> None:
    """
    Write publications search results to the output Excel file

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        publications_dfs_list_by_pub_type (list): list of DataFrames with search results by type
        patents (pd.DataFrame): patent application search results by filing date
        patent_applications (pd.DataFrame): patent search results by publication date
        author_profiles_by_ids_df (pd.DataFrame): author search results by ids
        author_profiles_by_names_df (pd.DataFrame): author search results by names

    Returns: None

    """

    # Load patents/applications and number of joint patents into author profiles
    (
        joint_patent_applications_by_author,
        joint_patent_applications_count,
        joint_patents_by_author,
        joint_patents_count,
    ) = _tabulate_patents_per_author(
        reference_query=reference_query,
        patent_applications=patent_applications,
        patents=patents,
    )
    author_profiles_by_ids_df["Brevets US (en instance)"] = (
        joint_patent_applications_by_author
    )
    author_profiles_by_ids_df["Brevets US (délivrés)"] = joint_patents_by_author

    # Create results summary dataframe
    results_df = _create_results_summary_df(
        reference_query=reference_query,
        publications_dfs_list_by_pub_type=publications_dfs_list_by_pub_type,
        patent_applications=patent_applications,
        joint_patent_applications_count=joint_patent_applications_count,
        patents=patents,
        joint_patents_count=joint_patents_count,
    )

    # Write dataframes in separate sheets to the output Excel file
    with pd.ExcelWriter(reference_query.out_excel_file) as writer:
        # Results (first) sheet
        results_df.to_excel(writer, index=False, header=False, sheet_name="Résultats")

        # Scopus search result sheets by publication type
        for i, df in enumerate(publications_dfs_list_by_pub_type):
            if not df.empty:
                # Remove singlets in co-publication count column
                joint_publication_counts: list[int] = [
                    count if count > 1 else None for count in df["Nb co-auteurs"].values
                ]
                df_copy = df.drop("Nb co-auteurs", axis=1).copy()
                df_copy["Nb co-auteurs"] = joint_publication_counts

                # Write dataframe to sheet
                _export_publications_df_to_excel_sheet(
                    writer=writer,
                    df=df_copy,
                    sheet_name=reference_query.publication_types[i],
                )

        # USPTO search result sheets
        if not patent_applications.empty:
            patent_applications.to_excel(
                writer, index=False, sheet_name="Brevets US (en instance)"
            )
        if not patents.empty:
            patents.to_excel(writer, index=False, sheet_name="Brevets US (délivrés)")

        # Author profile sheets
        col = author_profiles_by_ids_df.pop("Période active")
        author_profiles_by_ids_df["Période active"] = col
        author_profiles_by_ids_df.to_excel(
            writer, index=False, sheet_name="Auteurs - Profils"
        )
        author_profiles_by_names_df.to_excel(
            writer, index=False, sheet_name="Auteurs - Homonymes"
        )
    print(
        "Résultats de la recherche sauvegardés "
        f"dans le fichier '{reference_query.out_excel_file}'"
    )


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

    author_profiles_all_df = pd.DataFrame()
    for [lastname, firstname] in reference_query.au_names:
        query_string: str = f"AUTHLAST({lastname}) and AUTHFIRST({firstname})"
        author_profiles_from_name = AuthorSearch(
            query=query_string,
            refresh=reference_query.scopus_database_refresh,
            verbose=True,
        )
        if author_profiles_from_name.authors:
            author_profiles_from_name_df = pd.DataFrame(
                author_profiles_from_name.authors
            )
            author_profiles_from_name_df["eid"] = [
                au_id.split("-")[-1]
                for au_id in author_profiles_from_name_df.eid.to_list()
            ]
            (
                author_profiles_from_name_df["Start"],
                author_profiles_from_name_df["End"],
            ) = zip(
                *[
                    AuthorRetrieval(
                        author_id=au_id, refresh=reference_query.scopus_database_refresh
                    ).publication_range
                    for au_id in author_profiles_from_name_df.eid.to_list()
                ]
            )
            if not homonyms_only or author_profiles_from_name_df.shape[0] > 1:
                author_profiles_all_df = pd.concat(
                    [author_profiles_all_df, author_profiles_from_name_df],
                    ignore_index=True,
                )
                author_profiles_all_df.loc[len(author_profiles_all_df)] = [None] * len(
                    author_profiles_all_df.columns
                )
        elif not homonyms_only:
            print(
                f"[red]ERREUR: aucun résultat pour l'auteur.e '{lastname}, {firstname}' [/red]"
            )

    if not author_profiles_all_df.empty:
        author_profiles_all_df = _flag_matched_scopus_author_ids_and_affiliations(
            reference_query=reference_query, author_profiles_df=author_profiles_all_df
        )
        author_profiles_all_df = _reindex_author_profiles_df(df=author_profiles_all_df)

    return author_profiles_all_df


def query_scopus_author_profiles_by_id(reference_query: ReferenceQuery) -> pd.DataFrame:
    """

    Fetch author profiles from their IDs in the Scopus database

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object

    Returns: DataFrame with author profiles

    """

    columns: list[str] = [
        "Nom de famille",
        "Prénom",
        "Affiliation",
        "Affiliation mère",
        "Période active",
    ]
    author_profiles = []
    for i, au_id in enumerate(reference_query.au_ids):
        try:
            if au_id > 0:
                author = AuthorRetrieval(
                    author_id=au_id, refresh=reference_query.scopus_database_refresh
                )
                author_profiles.append(
                    [
                        author.surname,
                        author.given_name,
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
            print(
                f"[red]Erreur dans la recherche Scopus à la ligne {i + 2} "
                f"({reference_query.au_names[i][0]}, {reference_query.au_names[i][1]}) "
                f"du fichier {reference_query.in_excel_file}  - '{e}'[/red] - "
                f"Causes possibles: identifiant Scopus inconnu{vpn_required_str}!"
            )
            exit()

    return pd.DataFrame(author_profiles, columns=columns)


def query_scopus_publications(
    reference_query: ReferenceQuery,
) -> tuple[pd.DataFrame, list]:
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
    publications_df = pd.DataFrame()
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
                    refresh=reference_query.scopus_database_refresh,
                    verbose=True,
                )
            except ScopusException as e:
                print(
                    f"[red]Erreur dans la recherche Scopus pour l'identifiant {au_id}, "
                    f"causes possibles: identifiant inconnu ou tentative d'accès "
                    f"hors du réseau universitaire UdeS (VPN requis) - '{e}'[/red]"
                )
                exit()

            author_pubs_df = pd.DataFrame(query_results.results)
            pub_type_counts_by_author.append(
                _count_publications_by_type_in_df(
                    reference_query=reference_query, df=author_pubs_df
                )
            )
            publications_df = pd.concat([publications_df, author_pubs_df])
        else:
            pub_type_counts_by_author.append(
                [None] * len(reference_query.publication_type_codes)
            )

    if not publications_df.empty:
        publications_df = _add_coauthor_columns_and_clean_up_publications(
            publications_df, reference_query
        )

    return publications_df, pub_type_counts_by_author


def query_us_patents(
    reference_query: ReferenceQuery,
    applications: bool = True,
    application_ids_to_remove=None,
) -> tuple[pd.DataFrame, list]:
    """
    Query the USPTO database for patent applications and published patents
    for a list of authors over a range of years using the "patent_client" package

    See: https://patent-client.readthedocs.io/en/latest/user_guide/fulltext.html
         https://www.uspto.gov/patents/search/patent-public-search/quick-reference-guides

         USPTO database field codes for search over a range of years:
         - Applications: ((<first name>  NEAR2 <last name>).IN.) AND @AD>="<year0>0101"<="<year1>1231"
         - Patents: ((<first name> NEAR2 <last name>).IN.) AND @PD>="<year0>0101"<="<year1>1231"

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        applications (bool): Search filed applications if True, else search published patents if False
        application_ids_to_remove (list): List of application ids to remove from search results

    Returns : DataFrame with patent search results, list of patent application_ids

    """

    max_results: int = 500
    # Execute USPTO query (patent applications or delivered patents)
    if applications:
        query_str = _build_patent_query_string(
            reference_query=reference_query, field_code="AD"
        )
        patents = (
            PublishedApplication.objects.filter(query=query_str)
            .limit(max_results)
            .values(
                "app_filing_date",
                "guid",
                "appl_id",
                "patent_title",
                "inventors",
                "assignees",
            )
            .to_pandas()
        )

    else:
        query_str = _build_patent_query_string(
            reference_query=reference_query, field_code="PD"
        )
        patents = (
            Patent.objects.filter(query=query_str)
            .limit(max_results)
            .values(
                "publication_date",
                "app_filing_date",
                "guid",
                "appl_id",
                "patent_title",
                "inventors",
                "assignees",
            )
            .to_pandas()
        )

    # Clean up USPTO search results
    application_ids: list[int] = []
    if not patents.empty:
        # Loop to extract inventor and assignee lists (names only), flag patents
        # without at least one Canadian inventor to attempt to filter out patents with
        # inventors having same names as the authors but not being the same persons,
        # and double-check search results by inventor because the USPTO interface does
        # not handle multi-word names such as "Maude Josée" and will pick up all
        # authors with either "Maude" or "Josée" in their names.
        patents["local inventors"] = None
        patents["Nb co-inventors"] = None
        for i, row in patents.iterrows():
            patents.at[i, "inventors"] = [
                f'{row["inventors"][j][0][1]} ({row["inventors"][j][2][1]})'
                for j in range(len(row["inventors"]))
            ]
            validated_inventor_last_names: list[str] = [
                lastname
                for [lastname, firstname] in reference_query.au_names
                if any(
                    (
                        _to_lower_no_accents_no_hyphens(lastname)
                        in _to_lower_no_accents_no_hyphens(inventor)
                    )
                    and (
                        _to_lower_no_accents_no_hyphens(firstname)
                        in _to_lower_no_accents_no_hyphens(inventor)
                    )
                    for inventor in patents.iloc[i]["inventors"]
                )
            ]
            patents.at[i, "local inventors"] = validated_inventor_last_names
            patents.at[i, "Nb co-inventors"] = (
                len(validated_inventor_last_names)
                if len(validated_inventor_last_names) > 1
                else None
            )
            patents.at[i, "author_error"] = not validated_inventor_last_names
            patents.at[i, "assignees"] = [
                row["assignees"][j][2][1] for j in range(len(row["assignees"]))
            ]
            patents.at[i, "noCA"] = all(
                "(CA)" not in inventor for inventor in patents.iloc[i]["inventors"]
            )

        # Remove the rows with incorrect inventors flagged above
        patents.drop(patents[patents["author_error"]].index, inplace=True)
        patents.drop(columns=["author_error"], inplace=True)
        patents.drop(patents[patents["noCA"]].index, inplace=True)
        patents.drop(columns=["noCA"], inplace=True)

        # Compile list of application ids, then remove the un-needed ids column
        application_ids = patents["appl_id"].to_list()
        patents.drop(columns=["appl_id"], inplace=True)

        # Filter out applications for which patents have been delivered
        if applications and application_ids_to_remove:
            patents.drop(
                patents[
                    [
                        application_id in application_ids_to_remove
                        for application_id in application_ids
                    ]
                ].index,
                inplace=True,
            )

        # Reorder columns, change names to French, sort by date
        if applications:
            patents.sort_values(by=["app_filing_date"], inplace=True)
            patents.rename(
                columns={
                    "app_filing_date": "Date de dépôt",
                    "guid": "GUID",
                    "patent_title": "Titre",
                    "Nb co-inventors": "Nb co-inventeurs",
                    "local inventors": "Inventeurs locaux",
                    "inventors": "Inventeurs",
                    "assignees": "Cessionnaires",
                },
                inplace=True,
            )
            new_columns: list[str] = [
                "Date de dépôt",
                "GUID",
                "Titre",
                "Nb co-inventeurs",
                "Inventeurs locaux",
                "Inventeurs",
                "Cessionnaires",
            ]
        else:
            patents.rename(
                columns={
                    "publication_date": "Date de délivrance",
                    "app_filing_date": "Date de dépôt",
                    "guid": "GUID",
                    "patent_title": "Titre",
                    "Nb co-inventors": "Nb co-inventeurs",
                    "local inventors": "Inventeurs locaux",
                    "inventors": "Inventeurs",
                    "assignees": "Cessionnaires",
                },
                inplace=True,
            )
            new_columns: list[str] = [
                "Date de délivrance",
                "Date de dépôt",
                "GUID",
                "Titre",
                "Nb co-inventeurs",
                "Inventeurs locaux",
                "Inventeurs",
                "Cessionnaires",
            ]
            patents.sort_values(by=["Date de délivrance"], inplace=True)
        patents = patents[new_columns]

    return patents, application_ids


def query_publications_and_patents(reference_query: ReferenceQuery) -> None:
    """
    Search for publications in Scopus and patents in the USPTO database
    for a list of authors over a range of years

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: None

    """

    # Console banner
    print(
        "Recherche de publications et brevets US pour la période "
        f"{reference_query.pub_year_first}-{reference_query.pub_year_last}"
    )

    # Fetch author profiles corresponding to user-supplied Scopus IDs, check they match
    # the user-supplied names
    author_profiles_by_ids_df = query_scopus_author_profiles_by_id(
        reference_query=reference_query
    )
    author_profiles_by_ids_df.insert(
        loc=0,
        column="Erreurs",
        value=pd.Series(
            _check_author_name_correspondance(
                reference_query=reference_query, authors_df=author_profiles_by_ids_df
            )
        ),
    )

    # Fetch publications by type in the Scopus database, store in list of dataframes
    # by type
    publications_all_df, pub_type_counts_by_author = query_scopus_publications(
        reference_query=reference_query
    )

    # Loop to parse publications by type and store in separate dataframes in a list
    publications_dfs_list_by_pub_type: list[pd.DataFrame] = []
    if not publications_all_df.empty:
        for i, pub_type in enumerate(reference_query.publication_types):
            # Extract all publications of a given type, store in a separate dataframe
            # add the dataframe to the list of dataframes
            publications_dfs_list_by_pub_type.append(
                publications_all_df[
                    publications_all_df["subtype"]
                    == reference_query.publication_type_codes[i]
                ]
            )

            # If there were publications of this type, add the publication count
            # to the author profiles dataframe
            if len(publications_dfs_list_by_pub_type[i]) > 0:
                author_profiles_by_ids_df[pub_type] = [
                    row[i] if row[i] and row[i] > 0 else None for row in pub_type_counts_by_author
                ]
            print(f"{pub_type}: {len(publications_dfs_list_by_pub_type[i])}")

    # Fetch author Scopus profiles corresponding to user-supplied names, check for
    # author names with multiple Scopus IDs ("homonyms")
    author_profiles_by_name_df = query_scopus_author_profiles_by_name(
        reference_query=reference_query,
        homonyms_only=True,
    )

    # Fetch US patent applications and published patents
    patents, patent_application_ids = query_us_patents(
        reference_query=reference_query, applications=False
    )
    print("Brevets US (délivrés): ", len(patents))
    patent_applications, _ = query_us_patents(
        reference_query=reference_query,
        applications=True,
        application_ids_to_remove=patent_application_ids,
    )
    print("Brevets US (en instance): ", len(patent_applications))

    # Write results to output Excel file
    write_reference_query_results_to_excel(
        reference_query=reference_query,
        publications_dfs_list_by_pub_type=publications_dfs_list_by_pub_type,
        patents=patents,
        patent_applications=patent_applications,
        author_profiles_by_ids_df=author_profiles_by_ids_df,
        author_profiles_by_names_df=author_profiles_by_name_df,
    )


def query_author_profiles(reference_query: ReferenceQuery) -> None:
    """
    Query Scopus for a list of author profiles by name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: None

    """

    print("Recherche de profils d'auteur.e.s par nom dans la base de données Scopus")
    author_profiles_by_name_df = query_scopus_author_profiles_by_name(
        reference_query=reference_query,
        homonyms_only=False,
    )
    with pd.ExcelWriter(reference_query.out_excel_file) as writer:
        author_profiles_by_name_df.to_excel(writer, index=False, sheet_name="Profils")


def run_reference_search(reference_query: ReferenceQuery, search_type: str) -> None:
    """
     For a list of author names and range of years, search either for:
        - references (publications in Scopus, patents in the USPTO database)
          OR
        - author profiles in the Scopus database

     Args:
         reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
         search_type (str): Type of search ("Publications" or "Profils")

    Returns: None

    """

    # Console info starting messages
    python_version = (
        f"{str(sys.version_info.major)}"
        f".{str(sys.version_info.minor)}"
        f".{str(sys.version_info.micro)}"
    )
    print(f"{Path(__file__).stem} {__version__} " f"(running python {python_version})")
    print(
        f"Nombre d'auteur.e.s dans le fichier '{reference_query.in_excel_file}': "
        f"{len(reference_query.au_ids)}"
    )

    # Init Scopus API
    pybliometrics.scopus.init()

    # Run the query
    if search_type == "Publications":
        query_publications_and_patents(reference_query=reference_query)
    elif search_type == "Profils":
        query_author_profiles(reference_query=reference_query)
    else:
        print(
            f"[red]ERREUR: '{search_type}' est un type de recherche invalide, "
            "doit être 'Author name' ou 'Scopus ID'[/red]"
        )


def main():
    # Load search parameters from toml file
    toml_dict: dict = toml.load("pyRefSearchUdeS.toml")

    # Define input/output Excel file names
    in_excel_file: Path = Path(toml_dict["in_excel_file"])
    out_excel_file: Path = (
        Path(
            f"{in_excel_file.stem}_publications_"
            f"{toml_dict['pub_year_first']}-{toml_dict['pub_year_last']}"
            f"{in_excel_file.suffix}"
        )
        if toml_dict["search_type"] == "Publications"
        else Path(f"{in_excel_file.stem}_profils" f"{in_excel_file.suffix}")
    )

    # Define ReferenceQuery Class object containing the query parameters
    reference_query: ReferenceQuery = ReferenceQuery(
        in_excel_file=in_excel_file,
        in_excel_file_author_sheet=toml_dict["in_excel_file_author_sheet"],
        out_excel_file=out_excel_file,
        pub_year_first=toml_dict["pub_year_first"],
        pub_year_last=toml_dict["pub_year_last"],
        publication_types=toml_dict["publication_types"],
        local_affiliations=toml_dict["local_affiliations"],
        scopus_database_refresh=toml_dict["scopus_database_refresh"],
    )

    # Run the bibliographic search!
    run_reference_search(
        reference_query=reference_query,
        search_type=toml_dict["search_type"],
    )


if __name__ == "__main__":
    main()
    print("pyScopus terminé!")
