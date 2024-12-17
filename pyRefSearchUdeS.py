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

from itertools import chain
import math
import pybliometrics
import pandas as pd
from patent_client import Patent, PublishedApplication
from pathlib import Path
from pybliometrics.scopus import AuthorRetrieval, AuthorSearch, ScopusSearch
from pybliometrics.scopus.exception import ScopusException
from rich import print
import sys
import toml
import unidecode
from version import __version__


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
            _to_lower_no_accents(s) for s in local_affiliations
        ]
        self.scopus_database_refresh: bool | int = scopus_database_refresh

        # Check input/output Excel file access, script fails if files already open
        self.check_excel_file_access(self.in_excel_file)
        self.check_excel_file_access(self.out_excel_file)

        # Fetch list of author names from input Excel file
        self.au_names: list = pd.read_excel(
            self.in_excel_file,
            sheet_name=in_excel_file_author_sheet,
            usecols=["Nom", "Prénom"],
        ).values.tolist()

        # Fetch author Scopus IDs column from input Excel file, if it exists
        try:
            self.au_ids: list = pd.read_excel(
                self.in_excel_file,
                sheet_name=in_excel_file_author_sheet,
                usecols=["Scopus ID"],
            ).values.tolist()
            self.au_ids = list(chain.from_iterable(self.au_ids))
            self.au_ids = [0 if math.isnan(n) else int(n) for n in self.au_ids]
        except ValueError:
            self.au_ids = [0] * len(self.au_names)


def _to_lower_no_accents(s: str | pd.Series) -> str:
    """
    Convert string to lower case and remove accents

    Args:
        s (str): Input string

    Returns: String in lower case without accents

    """

    return unidecode.unidecode(s.lower().strip())


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

    local_column: list[str] = [""] * len(author_profiles_df)
    for i, row in author_profiles_df.iterrows():
        if row.affiliation is not None:
            local_affiliation_match: bool = any(
                s in _to_lower_no_accents(row.affiliation)
                for s in reference_query.local_affiliations
            )
            au_id_index: int | None = (
                reference_query.au_ids.index(int(row.eid))
                if int(row.eid) in reference_query.au_ids
                else None
            )
            au_id_match: bool = au_id_index is not None and _to_lower_no_accents(
                reference_query.au_names[au_id_index][0]
            ) == _to_lower_no_accents(row.surname)
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

    query_errors: list[str] = [""] * len(reference_query.au_ids)
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
            scopus_last_name: str = _to_lower_no_accents(
                authors_df["Nom de famille"][i]
            )
            if scopus_last_name != _to_lower_no_accents(input_last_name):
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
                else _to_lower_no_accents(authors_df["Affiliation"][i])
            )
            parent_affiliation: str = (
                ""
                if authors_df["Affiliation mère"][i] is None
                else _to_lower_no_accents(authors_df["Affiliation mère"][i])
            )
            if all(
                s not in affiliation and s not in parent_affiliation
                for s in reference_query.local_affiliations
            ):
                query_errors[i] = (
                    "Affiliation non locale"
                    if query_errors[i] == ""
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

    pub_type_counts: list = [0] * len(reference_query.publication_type_codes)
    if not df.empty:
        for i, pub_type in enumerate(reference_query.publication_type_codes):
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
        df[
            [
                "title",
                "author_names",
                "Coauteurs",
                "publicationName",
                "volume",
                "pageRange",
                "coverDisplayDate",
                "doi",
            ]
        ].to_excel(writer, index=False, sheet_name=sheet_name)


def _count_patents_per_author(
    reference_query: ReferenceQuery, patents: pd.DataFrame
) -> tuple[list, int]:
    """
    Count the number of patents for each author and the number of joint patents

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        patents (pd.DataFrame): patents or patent application search results

    Returns : Number of patents per author (list), number of joint publications

    """

    author_patent_counts: list[int] = []
    joint_patents: int = 0

    if not patents.empty:
        # Count patents per author
        for [lastname, firstname] in reference_query.au_names:
            author_patent_counts.append(
                sum(
                    [
                        1
                        for _, row in patents.iterrows()
                        if any(
                            [
                                _to_lower_no_accents(lastname)
                                in _to_lower_no_accents(inventor)
                                and _to_lower_no_accents(firstname)
                                in _to_lower_no_accents(inventor)
                                for inventor in row["inventors"]
                            ]
                        )
                    ]
                )
            )

        # Count joint patents
        for _, row in patents.iterrows():
            if (
                sum(
                    [
                        (
                            1
                            if any(
                                [
                                    _to_lower_no_accents(lastname)
                                    in _to_lower_no_accents(inventor)
                                    and _to_lower_no_accents(firstname)
                                    in _to_lower_no_accents(inventor)
                                    for inventor in row["inventors"]
                                ]
                            )
                            else 0
                        )
                        for [lastname, firstname] in reference_query.au_names
                    ]
                )
                > 1
            ):
                joint_patents += 1

    return author_patent_counts, joint_patents


def write_reference_query_results_to_excel(
    reference_query: ReferenceQuery,
    publications_by_type_dfs: list[pd.DataFrame],
    patents: pd.DataFrame,
    patent_applications: pd.DataFrame,
    author_profiles_by_ids_df: pd.DataFrame,
    author_profiles_by_names_df: pd.DataFrame,
) -> None:
    """
    Write publications search results to the output Excel file

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        publications_by_type_dfs (list): list of DataFrames with search results by type
        patents (pd.DataFrame): patent application search results by filing date
        patent_applications (pd.DataFrame): patent search results by publication date
        author_profiles_by_ids_df (pd.DataFrame): author search results by ids
        author_profiles_by_names_df (pd.DataFrame): author search results by names

    Returns: None

    """

    # Count number of patents & patent applications per author, and joint patents
    author_patent_application_counts, joint_patent_applications = (
        _count_patents_per_author(
            reference_query=reference_query, patents=patent_applications
        )
    )
    author_patent_counts, joint_patents = _count_patents_per_author(
        reference_query=reference_query, patents=patents
    )

    # Create results summary dataframe
    results: list[str] = [
        "",
        "Nb d'auteur.e.s",
        "Année de début",
        "Année de fin",
    ]
    results += reference_query.publication_types
    results += ["Brevets US (applications)", "Brevets US (délivrés)"]
    values: list = [
        "",
        len(reference_query.au_ids),
        reference_query.pub_year_first,
        reference_query.pub_year_last,
    ]
    if publications_by_type_dfs:
        values += [0 if df.empty else len(df) for df in publications_by_type_dfs]
    else:
        values += [0] * len(reference_query.publication_types)
    values += [
        len(patent_applications),
        len(patents),
    ]
    co_authors: list = ["Conjointes", "", "", ""]
    if publications_by_type_dfs:
        co_authors += [
            "" if df.empty else len(df[df["Coauteurs"] > 1])
            for df in publications_by_type_dfs
        ]
    else:
        co_authors += [0] * len(reference_query.publication_types)
    co_authors += [joint_patent_applications, joint_patents]
    results_df = pd.DataFrame([results, values, co_authors]).T

    # Write dataframes in separate sheets to the output Excel file
    with pd.ExcelWriter(reference_query.out_excel_file) as writer:
        # Results (first) sheet
        results_df.to_excel(writer, index=False, header=False, sheet_name="Résultats")

        # Scopus search publications sheets by type
        for i, df in enumerate(publications_by_type_dfs):
            if not df.empty:
                _export_publications_df_to_excel_sheet(
                    writer=writer,
                    df=df,
                    sheet_name=reference_query.publication_types[i],
                )

        # USPTO search results sheets
        if not patent_applications.empty:
            patent_applications.to_excel(
                writer, index=False, sheet_name="Brevets US (applications)"
            )
        if not patents.empty:
            patents.to_excel(writer, index=False, sheet_name="Brevets US (délivrés)")

        # Author profile sheets
        if author_patent_application_counts:
            author_profiles_by_ids_df["Brevets US (applications)"] = (
                author_patent_application_counts
            )
        if author_patent_counts:
            author_profiles_by_ids_df["Brevets US (délivrés)"] = author_patent_counts
        col = author_profiles_by_ids_df.pop("Période active")
        author_profiles_by_ids_df["Période active"] = col
        author_profiles_by_ids_df.to_excel(
            writer, index=False, sheet_name="Profils par identifiant"
        )
        author_profiles_by_names_df.to_excel(
            writer, index=False, sheet_name="Homonymes"
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

    Returns : DataFrame with publication search results, list of publication type counts by author
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
            query_results = ScopusSearch(
                query=query_str,
                refresh=reference_query.scopus_database_refresh,
                verbose=True,
            )
            author_pubs_df = pd.DataFrame(query_results.results)
            pub_type_counts_by_author.append(
                _count_publications_by_type_in_df(
                    reference_query=reference_query, df=author_pubs_df
                )
            )
            publications_df = pd.concat([publications_df, author_pubs_df])
        else:
            pub_type_counts_by_author.append(
                [0] * len(reference_query.publication_type_codes)
            )

    # Tabulate co-authors, remove duplicates and sort by title
    if not publications_df.empty:
        publications_df.sort_values(by=["eid"], inplace=True)
        co_authored = publications_df.pivot_table(
            columns=["eid"], aggfunc="size"
        ).values
        publications_df.drop_duplicates("eid", inplace=True)
        publications_df["Coauteurs"] = co_authored
        publications_df.sort_values(by=["title"], inplace=True)

    return publications_df, pub_type_counts_by_author


def query_us_patents(
    reference_query: ReferenceQuery, applications: bool = True
) -> pd.DataFrame:
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

    Returns : DataFrame with patent search results

    """

    max_results: int = 200
    if applications:
        # Execute filed applications query
        query_str = _build_patent_query_string(
            reference_query=reference_query, field_code="AD"
        )
        patents = (
            PublishedApplication.objects.filter(query=query_str)
            .limit(max_results)
            .values(
                "guid",
                "patent_title",
                "app_filing_date",
                "inventors",
                "assignees",
            )
            .to_pandas()
        )

    else:
        # Execute published patents query
        query_str = _build_patent_query_string(
            reference_query=reference_query, field_code="PD"
        )
        patents = (
            Patent.objects.filter(query=query_str)
            .limit(max_results)
            .values(
                "guid",
                "patent_title",
                "app_filing_date",
                "publication_date",
                "inventors",
                "assignees",
            )
            .to_pandas()
        )

    # Loop through results to extract lists of inventors and assignees, filter out
    # patents without Canadian inventors
    if not patents.empty:
        patents.assign(CA=False, inplace=True)
        for i, row in patents.iterrows():
            patents.at[i, "inventors"] = list(
                tuple(
                    f'{row["inventors"][j][0][1]} ({row["inventors"][j][2][1]})'
                    for j in range(len(row["inventors"]))
                )
            )
            patents.at[i, "assignees"] = list(
                tuple(row["assignees"][j][2][1] for j in range(len(row["assignees"])))
            )
            patents.at[i, "noCA"] = all(
                "(CA)" not in inventor for inventor in row["inventors"]
            )
        patents.drop(patents[patents["noCA"]].index, inplace=True)
        patents.drop(columns=["noCA"], inplace=True)
        patents.sort_values(by=["patent_title"], inplace=True)

    return patents


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
    publications_by_type_dfs: list[pd.DataFrame] = []
    if not publications_all_df.empty:
        for i, pub_type in enumerate(reference_query.publication_types):
            publications_by_type_dfs.append(
                publications_all_df[
                    publications_all_df["subtype"]
                    == reference_query.publication_type_codes[i]
                ]
            )
            if len(publications_by_type_dfs[i]) > 0:
                author_profiles_by_ids_df[pub_type] = [row[i] for row in pub_type_counts_by_author]
            print(f"{pub_type}: {len(publications_by_type_dfs[i])}")

    # Fetch author Scopus profiles corresponding to user-supplied names, check for
    # author names with multiple Scopus IDs ("homonyms")
    author_profiles_by_name_df = query_scopus_author_profiles_by_name(
        reference_query=reference_query,
        homonyms_only=True,
    )

    # Fetch US patent applications and published patents
    patent_applications: pd.DataFrame = query_us_patents(
        reference_query=reference_query, applications=True
    )
    print("Brevets US (applications): ", len(patent_applications))
    patents: pd.DataFrame = query_us_patents(
        reference_query=reference_query, applications=False
    )
    print("Brevets US (délivrés): ", len(patents))

    # Write results to output Excel file
    write_reference_query_results_to_excel(
        reference_query=reference_query,
        publications_by_type_dfs=publications_by_type_dfs,
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
