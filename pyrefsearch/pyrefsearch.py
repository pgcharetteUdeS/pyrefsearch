"""pyrefsearch.py

    For a list of author names and range of years supplied in an input Excel file, query:
    - references (publications in Scopus, patents in the INPADOC and USPTO databases)
      OR
    - author profiles (Scopus database), and write the results to an output Excel file.

    All execution parameters specified in the file "pyrefsearch.toml"

    The script uses the "pybliometrics" for Scopus searches,
    see https://pybliometrics.readthedocs.io/en/stable/
    NB: An API key is required to query the Scopus API,
        see https://dev.elsevier.com/index.jsp. The first execution of the script
        will prompt the user to enter the key.

    The script uses the "patent_client" package for searches in the USPTO
    and INPADOC databases, see https://patent-client.readthedocs.io/en/latest/index.html.
    NB: An API key is required to access INPADOC ("International Patent Documentation"
        database of patent information maintained by the European Patent Office,
        accessible via espacent), see pyrefsearch.toml.

    Project on gitHub: https://github.com/pgcharetteUdeS/pyRefSearchUdeS

"""

import argparse
import ast
import datetime
from datetime import timedelta
from openpyxl import load_workbook
import pybliometrics
import pandas as pd
from patent_client import Inpadoc, Patent, PublishedApplication
from pathlib import Path
import re
from rich import print
import sys
import time
import toml
from unidecode import unidecode

from search_scopus import (
    query_scopus_author_profiles_by_id,
    query_scopus_author_profiles_by_name,
    query_scopus_publications,
)
from referencequery import ReferenceQuery
from utils import to_lower_no_accents_no_hyphens
from version import __version__


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
        df_copy: pd.DataFrame = df.rename(
            columns={
                "coverDate": "Date",
                "title": "Titre",
                "Nb co-auteurs locaux": "Nb co-auteurs locaux",
                "Auteurs locaux": "Auteurs locaux",
                "author_names": "Auteurs",
                "publicationName": "Publication",
                "volume": "Volume",
                "pageRange": "Pages",
                "doi": "DOI",
            },
        ).copy()
        df_copy[
            [
                "Titre",
                "Date",
                "Nb co-auteurs locaux",
                "Auteurs locaux",
                "Auteurs",
                "Publication",
                "Volume",
                "Pages",
                "DOI",
            ]
        ].to_excel(writer, index=False, sheet_name=sheet_name, freeze_panes=(1, 1))


def _tabulate_patents_per_author(
    reference_query: ReferenceQuery,
    patents: pd.DataFrame,
) -> list:
    """
    Tabulate number of patents or patent applications per author

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        patents (pd.DataFrame): patent search results

    Returns: Number of patents or patent applications per author (list)

    """

    if patents.empty:
        return [None] * len(reference_query.au_ids)

    def inventor_match(inventors) -> bool:
        return any(
            to_lower_no_accents_no_hyphens(lastname)
            in to_lower_no_accents_no_hyphens(inventor)
            and to_lower_no_accents_no_hyphens(firstname)
            in to_lower_no_accents_no_hyphens(inventor)
            for inventor in inventors
        )

    author_patent_counts: list[int | None] = []
    for [lastname, firstname] in reference_query.au_names:
        count: int = sum(patents["Inventeurs"].apply(inventor_match))
        author_patent_counts.append(count if count > 0 else None)

    return author_patent_counts


def _query_uspto(
    reference_query: ReferenceQuery, applications: bool = True
) -> pd.DataFrame:
    """
    Query the USPTO database for patent applications or granted patents

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        applications (bool): Search filed applications if True, else search published patents if False

    Returns: DataFrame with patent search results

    """

    def inventor_query_str(inventor: list[str]) -> str:
        accented_chars: list[str] = ["é", "è", "ê", "ë", "É", "È", "Ê", "ç"]
        if any(c in inventor[0] or c in inventor[1] for c in accented_chars):
            return (
                f"({inventor[1]} NEAR2 {inventor[0]}) "
                f"OR ({unidecode(inventor[1])} NEAR2 {unidecode(inventor[0])})"
            )
        else:
            return f"({inventor[1]} NEAR2 {inventor[0]})"

    def build_uspto_patent_query_string(field_code: str) -> str:
        s: str = (
            f'@{field_code}>="{reference_query.pub_year_first}0101"'
            f'<="{reference_query.pub_year_last}1231" AND ('
        )
        s += inventor_query_str(reference_query.au_names[0])
        for name in reference_query.au_names[1:]:
            s += " OR "
            s += inventor_query_str(name)
        s += ")"
        return s

    max_results: int = 500
    patents: pd.DataFrame
    if applications:
        query_str: str = build_uspto_patent_query_string(field_code="AD")
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
                "related_apps",
            )
            .to_pandas()
        )

    else:
        query_str: str = build_uspto_patent_query_string(field_code="PD")
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
                "related_apps",
            )
            .to_pandas()
        )

    if not patents.empty:
        patents["appl_id"] = patents["appl_id"].astype(int)
    return patents


def _reformat_uspto_search_results(
    patents: pd.DataFrame, applications: bool
) -> pd.DataFrame:
    """
    Reorder USPTO search results by title, change column names to French, remove unnecessary columns

    Args:
        applications (bool): Search filed applications if True, else search published patents if False
        patents (pd.DataFrame): DataFrame with patent search results

    Returns: DataFrame with reordered search results

    """
    if applications:
        patents.rename(
            columns={
                "app_filing_date": "Date de dépôt",
                "guid": "GUID",
                "appl_id": "ID de l'application",
                "patent_title": "Titre",
                "Nb co-inventors": "Nb co-inventeurs locaux",
                "local inventors": "Inventeurs locaux",
                "inventors": "Inventeurs",
                "assignees": "Cessionnaires",
                "related_apps": "Applications liées",
            },
            inplace=True,
        )
        new_columns: list[str] = [
            "GUID",
            "Date de dépôt",
            "ID de l'application",
            "Titre",
            "Nb co-inventeurs locaux",
            "Inventeurs locaux",
            "Inventeurs",
            "Cessionnaires",
            "Applications liées",
        ]
    else:
        patents.rename(
            columns={
                "publication_date": "Date de délivrance",
                "app_filing_date": "Date de dépôt",
                "guid": "GUID",
                "appl_id": "ID de l'application",
                "patent_title": "Titre",
                "Nb co-inventors": "Nb co-inventeurs locaux",
                "local inventors": "Inventeurs locaux",
                "inventors": "Inventeurs",
                "assignees": "Cessionnaires",
                "related_apps": "Applications liées",
            },
            inplace=True,
        )
        new_columns: list[str] = [
            "GUID",
            "Date de délivrance",
            "Date de dépôt",
            "ID de l'application",
            "Titre",
            "Nb co-inventeurs locaux",
            "Inventeurs locaux",
            "Inventeurs",
            "Cessionnaires",
            "Applications liées",
        ]
    patents = patents.sort_values(by=["Titre"])

    return patents[new_columns]


def _create_results_summary_df(
    reference_query: ReferenceQuery,
    publications_dfs_list_by_pub_type: list,
    uspto_patent_applications: pd.DataFrame,
    uspto_patents: pd.DataFrame,
    inpadoc_patent_applications: pd.DataFrame,
    inpadoc_patents: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create results summary dataframe
    - "results" (first column): search information labels and bibliographic item names
    - "values" (second column): search information and result counts
    - "co_authors" (third column): number of local co-authors for each publication type

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        publications_dfs_list_by_pub_type (list): list of DataFrames with search results by type
        uspto_patent_applications (pd.DataFrame): uspto patent application search results
        uspto_patents (pd.DataFrame): uspto patent search results
        inpadoc_patent_applications (pd.DataFrame): inpadoc patent search results
        inpadoc_patents (pd.DataFrame): inpadoc patent search results

    Returns: DataFrame with results summary

    """

    # Initialize 3 columns contents
    results: list = [
        None,
        "Nb d'auteur.e.s",
        "Année de début",
        "Année de fin",
    ]
    values: list = [
        None,
        len(reference_query.au_ids),
        reference_query.pub_year_first,
        reference_query.pub_year_last,
    ]
    co_authors: list = ["Conjointes", None, None, None]

    # Publications search results
    results += reference_query.publication_types
    if publications_dfs_list_by_pub_type:
        values += [
            0 if df.empty else len(df) for df in publications_dfs_list_by_pub_type
        ]
        co_authors += [
            None if df.empty else len(df[df["Nb co-auteurs locaux"] > 1])
            for df in publications_dfs_list_by_pub_type
        ]
    else:
        values += [None] * len(reference_query.publication_types)
        co_authors += [None] * len(reference_query.publication_types)

    # USPTO search results
    if not uspto_patents.empty or not uspto_patent_applications.empty:
        results += ["Brevets USPTO (en instance)", "Brevets USPTO (délivrés)"]
        values += [
            len(uspto_patent_applications),
            len(uspto_patents),
        ]
        uspto_joint_patent_applications_count: int = sum(
            row["Nb co-inventeurs locaux"] is not None
            and row["Nb co-inventeurs locaux"] > 1
            for _, row in uspto_patent_applications.iterrows()
        )
        uspto_joint_patents_count: int = sum(
            row["Nb co-inventeurs locaux"] is not None
            and row["Nb co-inventeurs locaux"] > 1
            for _, row in uspto_patents.iterrows()
        )
        co_authors += [uspto_joint_patent_applications_count, uspto_joint_patents_count]

    # INPADOC search results
    if not inpadoc_patents.empty or not inpadoc_patent_applications.empty:
        results += ["Brevets INPADOC (en instance)", "Brevets INPADOC (délivrés)"]
        values += [
            len(inpadoc_patent_applications),
            len(inpadoc_patents),
        ]
        inpadoc_patent_applications_count: int = sum(
            row["Nb co-inventeurs locaux"] is not None
            and row["Nb co-inventeurs locaux"] > 1
            for _, row in inpadoc_patent_applications.iterrows()
        )
        inpadoc_patents_count: int = sum(
            row["Nb co-inventeurs locaux"] is not None
            and row["Nb co-inventeurs locaux"] > 1
            for _, row in inpadoc_patents.iterrows()
        )
        co_authors += [inpadoc_patent_applications_count, inpadoc_patents_count]

    return pd.DataFrame([results, values, co_authors]).T


def write_reference_query_results_to_excel(
    reference_query: ReferenceQuery,
    publications_dfs_list_by_pub_type: list[pd.DataFrame],
    uspto_patents: pd.DataFrame,
    uspto_patent_applications: pd.DataFrame,
    inpadoc_patents: pd.DataFrame,
    inpadoc_patent_applications: pd.DataFrame,
    author_profiles_by_ids: pd.DataFrame,
    author_profiles_by_name: pd.DataFrame,
) -> None:
    """
    Write publications search results to the output Excel file

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        publications_dfs_list_by_pub_type (list): list of DataFrames with search results by type
        uspto_patents (pd.DataFrame): USPTO patent application search results
        uspto_patent_applications (pd.DataFrame): USPTO patent search results
        inpadoc_patents (pd.DataFrame): INPADOC patent search result
        inpadoc_patent_applications (pd.DataFrame): INPADOC patent search results
        author_profiles_by_ids (pd.DataFrame): author search results by ids
        author_profiles_by_name (pd.DataFrame): author search results by names

    Returns: None

    """

    # Create results summary dataframe
    results: pd.DataFrame = _create_results_summary_df(
        reference_query=reference_query,
        publications_dfs_list_by_pub_type=publications_dfs_list_by_pub_type,
        uspto_patent_applications=uspto_patent_applications,
        uspto_patents=uspto_patents,
        inpadoc_patent_applications=inpadoc_patent_applications,
        inpadoc_patents=inpadoc_patents,
    )

    # Write dataframes in separate sheets to the output Excel file
    with pd.ExcelWriter(reference_query.out_excel_file) as writer:
        # Results (first) sheet
        results.to_excel(writer, index=False, header=False, sheet_name="Résultats")

        # Write Scopus search results dataframes to separate sheets by publication type
        for df, pub_type in zip(
            publications_dfs_list_by_pub_type, reference_query.publication_types
        ):
            if not df.empty:
                _export_publications_df_to_excel_sheet(
                    writer=writer,
                    df=df,
                    sheet_name=pub_type,
                )

        # USPTO search result sheets
        if not uspto_patent_applications.empty:
            uspto_patent_applications.to_excel(
                writer,
                index=False,
                sheet_name="Brevets US (en instance)",
                freeze_panes=(1, 1),
            )
        if not uspto_patents.empty:
            uspto_patents.to_excel(
                writer,
                index=False,
                sheet_name="Brevets US (délivrés)",
                freeze_panes=(1, 1),
            )

        # INPADOC search result sheets
        if not inpadoc_patent_applications.empty:
            inpadoc_patent_applications.to_excel(
                writer,
                index=False,
                sheet_name="Brevets INPADOC (en instance)",
                freeze_panes=(1, 1),
            )
        if not inpadoc_patents.empty:
            inpadoc_patents.to_excel(
                writer,
                index=False,
                sheet_name="Brevets INPADOC (délivrés)",
                freeze_panes=(1, 1),
            )

        # Author profile sheets
        col: pd.Series = author_profiles_by_ids.pop("Période active")
        author_profiles_by_ids["Période active"] = col
        author_profiles_by_ids.to_excel(
            writer, index=False, sheet_name="Auteurs - Profils", freeze_panes=(1, 1)
        )
        author_profiles_by_name.to_excel(
            writer, index=False, sheet_name="Auteurs - Homonymes", freeze_panes=(1, 1)
        )
    print(
        "Résultats de la recherche sauvegardés "
        f"dans le fichier '{reference_query.out_excel_file}'"
    )

    # Attempt to adjust column widths in the output Excel file to reasonable values.
    # The solution is a hack because the auto_size/bestFit properties in
    # openpyxl.worksheet.dimensions.ColumnDimension() don't seem to work and the actual
    # column width sizing in Excel is system-dependant and a bit of a black box.
    workbook = load_workbook(reference_query.out_excel_file)
    col_width_max: int = 100
    for sheet_name in workbook.sheetnames:
        for i, col in enumerate(workbook[sheet_name].columns):
            # workbook[sheet_name].column_dimensions[col[0].column_letter].bestFit = True
            col_width: int = int(max(len(str(cell.value)) for cell in col) * 0.85)
            col_width_min: int = 18 if i == 0 else 10
            workbook[sheet_name].column_dimensions[col[0].column_letter].width = max(
                min(col_width_max, col_width), col_width_min
            )
    workbook.save(reference_query.out_excel_file)


def query_uspto_patents_and_applications(
    reference_query: ReferenceQuery,
    applications: bool = True,
    application_ids_to_remove=None,
) -> tuple[pd.DataFrame, list, list]:
    """
    Query the USPTO database for patent applications or published patents
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

    # Execute USPTO query (patent applications or delivered patents)
    if applications:
        print("En attente de la recherche USPTO de brevets en instance...", end="")
    else:
        print("En attente de la recherche USPTO de brevets délivrés...", end="")
    patents: pd.DataFrame = _query_uspto(
        reference_query=reference_query, applications=applications
    )
    print("terminé!")

    # Clean up USPTO search result dataframes
    application_ids: list[int] = []
    patent_counts_by_author: list[int | None] = [None] * len(reference_query.au_ids)
    if not patents.empty:
        # Simplify lists of inventors (names + country codes) and assignees (names)
        patents["inventors"] = patents["inventors"].apply(
            lambda inventors: [
                f"{inventor[0][1]} ({inventor[2][1]})" for inventor in inventors
            ]
        )
        patents["assignees"] = patents["assignees"].apply(
            lambda assignees: [assignee[2][1] for assignee in assignees]
        )

        # Remove dataframe rows with no Canadian inventors
        no_canadian_inventors: pd.Series = patents["inventors"].apply(
            lambda inventors: all("(CA)" not in inventor for inventor in inventors)
        )
        patents.drop(patents[no_canadian_inventors].index, inplace=True)

        # Add dataframe columns with lists and counts of local inventors
        patents["local inventors"] = patents["inventors"].apply(
            lambda inventors: [
                lastname
                for [lastname, firstname] in reference_query.au_names
                if any(
                    (
                        to_lower_no_accents_no_hyphens(lastname)
                        in to_lower_no_accents_no_hyphens(inventor)
                    )
                    and (
                        to_lower_no_accents_no_hyphens(firstname)
                        in to_lower_no_accents_no_hyphens(inventor)
                    )
                    for inventor in inventors
                )
            ]
        )
        patents["Nb co-inventors"] = patents["local inventors"].apply(
            lambda inventors: len(inventors) if len(inventors) > 1 else None
        )

        # Remove dataframe rows with no local inventors
        no_local_inventors: pd.Series = patents["local inventors"].apply(
            lambda inventors: not inventors
        )
        patents.drop(patents[no_local_inventors].index, inplace=True)

        # Remove applications for which patents have been delivered, i.e.
        # patent applications having same "appl_id" as delivered patents.
        # Compile list of patent/application ids before removal (used later)
        application_ids: list = patents["appl_id"].to_list()
        if applications and application_ids_to_remove:
            mask = patents["appl_id"].isin(application_ids_to_remove)
            patents.drop(patents[mask].index, inplace=True)

        # Reorder columns, change names to French, sort by title
        patents = _reformat_uspto_search_results(
            patents=patents, applications=applications
        )

        # Tabulate number of patents or patent applications per author
        patent_counts_by_author = _tabulate_patents_per_author(
            reference_query=reference_query, patents=patents
        )

    return patents, application_ids, patent_counts_by_author


def _add_local_inventors_column_to_df(
    reference_query: ReferenceQuery, patents_df: pd.DataFrame, column: str
) -> None:
    """
    Add column with lists of local inventors to dataframe

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        patents_df (pd.DataFrame): DataFrame with patent information
        column (str): Column name with inventors

    """

    # Add dataframe column with lists of local inventors
    local_inventors = patents_df[column].apply(
        lambda inventors: (
            [
                lastname
                for [lastname, firstname] in reference_query.au_names
                if any(
                    (
                        to_lower_no_accents_no_hyphens(lastname)
                        in to_lower_no_accents_no_hyphens(inventor)
                    )
                    and (
                        to_lower_no_accents_no_hyphens(firstname)
                        in to_lower_no_accents_no_hyphens(inventor)
                    )
                    for inventor in inventors
                )
            ]
            if len(inventors) > 1
            else None
        )
    )
    patents_df.insert(
        loc=patents_df.columns.get_loc(column),
        column="Local inventors",
        value=local_inventors,
    )


def _fetch_inpadoc_patent_family_members(member_info) -> tuple[list, list]:
    # Start the list of member patent info for this family with the parent
    family_member_patent_ids: list = [member_info.application_number]
    family_member_publication_dates: list = [
        str(member_info.publication_reference_epodoc.date)
    ]

    # Add remaining family member patent info to the list
    for member in member_info.family:
        family_member_patent_ids.append(member.publication_number)
        family_member_publication_dates.append(
            str(member.publication_reference[0]["date"])
        )

    # Prune the lists to keep only patents from relevant countries (US, CA, WO)
    family_member_patent_ids_filtered: list = []
    family_member_publication_dates_filtered: list = []
    for i, pid in enumerate(family_member_patent_ids):
        if pid[:2] in ["US", "CA", "WO"]:
            family_member_patent_ids_filtered.append(pid)
            family_member_publication_dates_filtered.append(
                family_member_publication_dates[i]
            )

    return family_member_patent_ids_filtered, family_member_publication_dates_filtered


def _extract_earliest_inpadoc_patent_family_members(patent_families: pd.DataFrame):
    """
    Extract earliest patent application and granted patent from patent families

    Args:
        patent_families (pd.DataFrame): DataFrame with patent family information

    Return: none

    """

    earliest_application_dates: list = []
    earliest_application_numbers: list = []
    earliest_granting_dates: list = []
    earliest_granting_numbers: list = []
    for _, row in patent_families.iterrows():
        earliest_application_date: str = ""
        earliest_application_number: str = ""
        earliest_granting_date: str = ""
        earliest_granting_number: str = ""
        for date, pid in zip(row["Dates de publication"], row["Numéros de brevet"]):
            if "B" in pid[-2:] or "C" in pid[-2:]:
                if not earliest_granting_date or date < earliest_granting_date:
                    earliest_granting_date = date
                    earliest_granting_number = pid
            elif (
                not earliest_application_date
                or date < earliest_application_date
                or (date == earliest_application_date and pid[:2] == "WO")
            ):
                earliest_application_date = date
                earliest_application_number = pid
        earliest_granting_dates.append(earliest_granting_date)
        earliest_granting_numbers.append(earliest_granting_number)
        earliest_application_dates.append(earliest_application_date)
        earliest_application_numbers.append(earliest_application_number)

    # Load earliest application and granted patents into patent families dataframe
    patent_families["Prémier dépôt"] = earliest_application_numbers
    patent_families["Date de dépôt"] = earliest_application_dates
    patent_families["Premier brevet délivré"] = earliest_granting_numbers
    patent_families["Date de délivrance"] = earliest_granting_dates


def _fetch_inpadoc_patent_families_by_author_name(
    last_name: str, first_name: str
) -> pd.DataFrame:
    """
    Fetch INPADOC patent family IDs for author

    Args:
        last_name (str): Last name of author
        first_name (str): First name of author

    Returns: DataFrame with unique INPADOC patent family & patent IDs

    """

    def inventor_query_str() -> str:
        """
        Build inventor query string that convers all combinations
        of first and last names that may contain hyphens
        """
        query_str: str
        if "-" in last_name and "-" in first_name:
            last_name0, last_name1 = last_name.split("-")
            first_name0, first_name1 = first_name.split("-")
            query_str = (
                f'(in=("{last_name0}" prox/distance<1 "{last_name1}")'
                f' AND in=("{last_name0}" prox/distance<2 "{first_name0}")'
                f' AND in=("{last_name0}" prox/distance<2 "{first_name1}"))'
            )
        elif "-" in last_name:
            last_name0, last_name1 = last_name.split("-")
            query_str = (
                f'(in=("{last_name0}" prox/distance<1 "{last_name1}")'
                f' AND in=("{last_name0}" prox/distance<2 "{first_name}")'
                f' AND in=("{last_name0}" prox/distance<2 "{first_name}"))'
            )
        elif "-" in first_name:
            first_name0, first_name1 = first_name.split("-")
            query_str = (
                f'(in=("{last_name}" prox/distance<2 "{first_name0}")'
                f' AND in=("{last_name}" prox/distance<2 "{first_name1}")'
                f' AND in=("{first_name0}" prox/distance<1 "{first_name1}"))'
            )
        else:
            query_str = f'in=("{last_name}" prox/distance<1 "{first_name}")'
        return query_str

    patents = Inpadoc.objects.filter(cql_query=inventor_query_str()).to_pandas()
    patents_name_list: list[dict] = []
    for _, row in patents.iterrows():
        patent_id_info: dict = dict(row.values)
        patent_id_info["patent_id"] = (
            f"{patent_id_info['country']}{patent_id_info['doc_number']}{patent_id_info['kind']}"
        )
        patent_id_info.pop("country")
        patent_id_info.pop("doc_number")
        patent_id_info.pop("kind")
        patent_id_info.pop("id_type")
        patents_name_list.append(patent_id_info)
    patents_name_df = pd.DataFrame(patents_name_list)

    return patents_name_df.drop_duplicates(subset=["family_id"])


def _search_espacenet_by_author_name(reference_query: ReferenceQuery) -> pd.DataFrame:
    """
    Search the INPADOC worldwide patent library via espacenet by author name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: DataFrame with patent family information

    """

    # Fetch unique patent families by author name, add delay so that search is not blocked
    patent_families_raw: pd.DataFrame = pd.DataFrame([])
    print(f"Recherche dans espacenet des {len(reference_query.au_names)} inventeurs...")
    for name in reference_query.au_names:
        lastname, firstname = name
        patent_families_raw = pd.concat(
            [
                patent_families_raw,
                _fetch_inpadoc_patent_families_by_author_name(
                    last_name=lastname, first_name=firstname
                ),
            ],
            ignore_index=True,
        )
    patent_families_raw = patent_families_raw.drop_duplicates(subset=["family_id"])
    patent_families_raw = patent_families_raw.reset_index(drop=True)

    # Fetch detailed patent family info
    families: list = []
    titles: list = []
    inventors: list = []
    applicants: list = []
    patent_ids: list[list] = []
    publication_dates: list[list] = []
    print(
        f"Analyze dans espacenet des {len(patent_families_raw.index)} familles de brevets..."
    )
    for i, row in patent_families_raw.iterrows():
        print(f"{row['family_id']} ({i}/{len(patent_families_raw.index)})", end=", ")
        if not hash(i) % 10 and hash(i) > 0:
            print("")
        member_info = Inpadoc.objects.get(row["patent_id"])
        if any("[CA]" in s for s in member_info.inventors_epodoc) and member_info.title:
            # Store tile, inventors, and applicants for this family
            families.append(member_info.family_id)
            titles.append(member_info.title)
            inventors.append(member_info.inventors_original)
            applicants.append(member_info.applicants_original)

            # Store patent member info for this family
            (
                family_member_patent_ids_filtered,
                family_member_publication_dates_filtered,
            ) = _fetch_inpadoc_patent_family_members(member_info)
            patent_ids.append(family_member_patent_ids_filtered)
            publication_dates.append(family_member_publication_dates_filtered)
    print("")

    # Create dataframe with patent family info
    patent_families: pd.DataFrame = pd.DataFrame(families, columns=["Famille"])
    patent_families["Titre"] = titles
    patent_families["Inventeurs"] = inventors
    patent_families["Cessionnaires"] = applicants
    patent_families["Numéros de brevet"] = patent_ids
    patent_families["Dates de publication"] = publication_dates

    # Add earliest patent application and granted patent to the patent families dataframe
    _extract_earliest_inpadoc_patent_family_members(patent_families)

    # Sort family dataframe by title
    patent_families = patent_families.sort_values(by=["Titre"])

    # Write dataframe of all patent results to output Excel file
    with pd.ExcelWriter(
        reference_query.data_dir
        / Path(f"espacenet_INPADOC_results_{time.strftime('%Y%m%d')}.xlsx")
    ) as writer:
        patent_families.to_excel(
            writer,
            index=False,
            header=True,
            sheet_name="Recherche par inventeurs",
            freeze_panes=(1, 1),
        )

    # Return dataframe of search results
    return patent_families


def _load_inpadoc_search_results_from_excel_file(
    reference_query: ReferenceQuery,
) -> pd.DataFrame:
    """
    Load previous INPADOC search results from Excel file, where search date is in the
    file name <filename>YYYYMMDD.xlsx

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: DataFrame with INPADOC search results

    """

    # Extract date from file name, show warning on console if file is older than 30 days
    if not (
        match := re.search(
            r"(\d{8}).xlsx",
            reference_query.espacenet_patent_search_results_file,
        )
    ):
        raise ValueError(
            "Impossible d'extraire la date du fichier de résultats de recherche"
            f" '{reference_query.espacenet_patent_search_results_file}' "
            "qui doit être en format '<filename>YYYYMMDD.xlsx'!"
        )
    file_date = datetime.datetime.strptime(match[1], "%Y%m%d").date()
    if datetime.date.today() - file_date >= timedelta(days=30):
        print(
            "[yellow]WARNING: Les données dans le fichier "
            f"'{reference_query.espacenet_patent_search_results_file}' "
            "ont plus de 30 jours![/yellow]"
        )

    # Load data from Excel file
    patent_families: pd.DataFrame = pd.read_excel(
        reference_query.data_dir
        / Path(reference_query.espacenet_patent_search_results_file)
    )

    def parse_list_field(value):
        """
        Attempts to parse a string representation of a list.
        First, it tries using ast.literal_eval. If that fails,
        it falls back to regex-based extraction.
        """
        try:
            parsed_value = ast.literal_eval(value)
            if isinstance(parsed_value, list):
                return parsed_value
        except (ValueError, SyntaxError):
            # Fall back to extracting items between apostrophes.
            return re.findall(r"'([^']+)'", value)
        return []

    # Reformat inventors and applicants columns into proper lists using the robust parser
    patent_families["Inventeurs"] = patent_families["Inventeurs"].apply(
        parse_list_field
    )
    patent_families["Cessionnaires"] = patent_families["Cessionnaires"].apply(
        parse_list_field
    )

    return patent_families


def query_espacenet_patents_and_applications(
    reference_query: ReferenceQuery,
) -> tuple[pd.DataFrame, list, pd.DataFrame, list]:
    """

    Query the INPADOC worldwide patent library via espacenet. INPADOC (International
    Patent Documentation) is a free database of patent information. The European
    Patent Office (EPO) produces and maintains the database.

    To connect to the INPADOC worldwide patent search services, API keys are
    required and must then be defined locally as environmental variables, see:
      - https://patent-client.readthedocs.io/en/stable/getting_started.html
      - https://www.epo.org/en/searching-for-patents/data/web-services/ops

    Espacenet/Inpadoc search, see:
        https://link.epo.org/web/technical/espacenet/espacenet-pocket-guide-en.pdf
        New interface: https://worldwide.espacenet.com/patent/search
        Old interface: https://worldwide.espacenet.com/?locale=en_EP
        NB: Old interface does not accept hyphens, which seems to be the case for
            this library!

        Example search string:
          '(in=("charette" prox/distance<1 "paul") OR in=("hunter" prox/distance<1 "ian")) AND pd within "1990,2020"'

        Because granted patents don't come up in espacenet search by year, must search
        for patents by author name and then filter by year in post.

    """

    # Search espacenet or get previous research results from file
    if reference_query.espacenet_patent_search_results_file:
        patent_families: pd.DataFrame = _load_inpadoc_search_results_from_excel_file(
            reference_query
        )
    else:
        # else, search espacenet for patent families by author name, save to file
        patent_families: pd.DataFrame = _search_espacenet_by_author_name(
            reference_query
        )

    # Add columns with local inventors and number of co-inventors to the dataframe
    local_inventors = patent_families["Inventeurs"].apply(
        lambda inventors: [
            lastname
            for [lastname, firstname] in reference_query.au_names
            if any(
                (
                    to_lower_no_accents_no_hyphens(lastname)
                    in to_lower_no_accents_no_hyphens(inventor)
                )
                and (
                    to_lower_no_accents_no_hyphens(firstname)
                    in to_lower_no_accents_no_hyphens(inventor)
                )
                for inventor in inventors
            )
        ]
    )
    patent_families.insert(loc=2, column="Inventeurs locaux", value=local_inventors)
    local_inventors_cnt = patent_families["Inventeurs locaux"].apply(
        lambda inventors: len(inventors) if len(inventors) > 1 else None
    )
    patent_families.insert(
        loc=3, column="Nb co-inventeurs locaux", value=local_inventors_cnt
    )
    patent_families = patent_families.drop(
        patent_families[patent_families["Inventeurs locaux"].map(len) == 0].index
    )

    # Extract patent application and granted patent by date, add columns to dataframe
    applications_published_in_year_range: pd.DataFrame = patent_families[
        (patent_families["Date de dépôt"] >= f"{reference_query.pub_year_first}-01-01")
        & (patent_families["Date de dépôt"] <= f"{reference_query.pub_year_last}-12-31")
    ]
    patents_granted_in_year_range: pd.DataFrame = patent_families[
        (
            patent_families["Date de délivrance"]
            >= f"{reference_query.pub_year_first}-01-01"
        )
        & (
            patent_families["Date de délivrance"]
            <= f"{reference_query.pub_year_last}-12-31"
        )
    ]

    # Tabulate number of patents and patent applications per author
    patent_application_counts_by_author: list = _tabulate_patents_per_author(
        reference_query=reference_query, patents=applications_published_in_year_range
    )
    patent_granted_counts_by_author: list = _tabulate_patents_per_author(
        reference_query=reference_query, patents=patents_granted_in_year_range
    )

    # Return dataframe for INPADOC patent applications and granted patents
    return (
        applications_published_in_year_range,
        patent_application_counts_by_author,
        patents_granted_in_year_range,
        patent_granted_counts_by_author,
    )


def query_publications_and_patents(reference_query: ReferenceQuery) -> None:
    """
    Search for publications in Scopus and patents in the USPTO & INPADOC databases
    for a list of authors over a range of years

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: None

    """

    # Console banner
    print(
        "Recherche de publications et brevets pour la période "
        f"{reference_query.pub_year_first}-{reference_query.pub_year_last}"
    )

    # Fetch author profiles corresponding to user-supplied Scopus IDs, check they match
    # the user-supplied names, flag any inconsistencies in the "Erreurs" column
    author_profiles_by_ids: pd.DataFrame = query_scopus_author_profiles_by_id(
        reference_query=reference_query
    )

    # Fetch publications by type in Scopus database, count publication types by author
    publications_all: pd.DataFrame
    pub_type_counts_by_author: list[list[int | None]]
    publications_all, pub_type_counts_by_author = query_scopus_publications(
        reference_query=reference_query
    )

    # Loop to parse publications by type into separate dataframes, store dfs in a list
    publications_dfs_list_by_pub_type: list[pd.DataFrame] = []
    if not publications_all.empty:
        for [pub_type, pub_code, pub_counts] in zip(
            reference_query.publication_types,
            reference_query.publication_type_codes,
            pub_type_counts_by_author,
        ):
            # Extract "pub_type" publications into a dataframe, add dataframe to list
            df: pd.DataFrame = publications_all[publications_all["subtype"] == pub_code]
            publications_dfs_list_by_pub_type.append(df)
            print(f"{pub_type}: {len(df)}")

            # Add "pub_type" publication counts to the author profiles
            if len(df) > 0:
                author_profiles_by_ids[pub_type] = pub_counts

    # Fetch USPTO applications and granted patents into separate dataframes, if required
    uspto_patents: pd.DataFrame = pd.DataFrame()
    uspto_patent_applications: pd.DataFrame = pd.DataFrame()
    if reference_query.uspto_patent_search:
        uspto_patent_application_ids: list
        uspto_patent_counts_by_author: list
        uspto_patents, uspto_patent_application_ids, uspto_patent_counts_by_author = (
            query_uspto_patents_and_applications(
                reference_query=reference_query, applications=False
            )
        )
        print("Brevets US (délivrés): ", len(uspto_patents))
        uspto_patent_application_counts_by_author: list
        uspto_patent_applications, _, uspto_patent_application_counts_by_author = (
            query_uspto_patents_and_applications(
                reference_query=reference_query,
                applications=True,
                application_ids_to_remove=uspto_patent_application_ids,
            )
        )
        print("Brevets US (en instance): ", len(uspto_patent_applications))

        # Add patent application and published patent counts to the author profiles
        author_profiles_by_ids["Brevets US (en instance)"] = (
            uspto_patent_application_counts_by_author
        )
        author_profiles_by_ids["Brevets US (délivrés)"] = uspto_patent_counts_by_author

    # Fetch INPADOC applications and granted patents into separate dataframes, if required
    inpadoc_patent_applications = pd.DataFrame()
    inpadoc_patents = pd.DataFrame()
    if reference_query.espacenet_patent_search:
        (
            inpadoc_patent_applications,
            inpadoc_patent_application_counts_per_author,
            inpadoc_patents,
            inpadoc_patent_counts_per_author,
        ) = query_espacenet_patents_and_applications(reference_query)
        author_profiles_by_ids["Brevets INPADOC (en instance)"] = (
            inpadoc_patent_application_counts_per_author
        )
        author_profiles_by_ids["Brevets INPADOC (délivrés)"] = (
            inpadoc_patent_counts_per_author
        )
        print("Brevets INPADOC en instance: ", len(inpadoc_patent_applications))
        print("Brevets INPADOC délivrés: ", len(inpadoc_patents))

    # Fetch Scopus author profiles corresponding to user-supplied names, check for
    # author names with multiple Scopus IDs ("homonyms"), load into dataframe
    author_profiles_by_name: pd.DataFrame = query_scopus_author_profiles_by_name(
        reference_query=reference_query,
        homonyms_only=True,
    )

    # Write results to output Excel file
    write_reference_query_results_to_excel(
        reference_query=reference_query,
        publications_dfs_list_by_pub_type=publications_dfs_list_by_pub_type,
        uspto_patents=uspto_patents,
        uspto_patent_applications=uspto_patent_applications,
        inpadoc_patents=inpadoc_patents,
        inpadoc_patent_applications=inpadoc_patent_applications,
        author_profiles_by_ids=author_profiles_by_ids,
        author_profiles_by_name=author_profiles_by_name,
    )


def query_author_profiles(reference_query: ReferenceQuery) -> None:
    """
    Query Scopus for a list of author profiles by name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: None

    """

    print("Recherche de profils d'auteur.e.s par nom dans la base de données Scopus")
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
    print(
        "Résultats de la recherche sauvegardés "
        f"dans le fichier '{reference_query.out_excel_file}'"
    )


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
    # Console info starting messages
    python_version: str = (
        f"{str(sys.version_info.major)}"
        f".{str(sys.version_info.minor)}"
        f".{str(sys.version_info.micro)}"
    )
    print(f"{Path(__file__).stem} {__version__} " f"(running python {python_version})")

    # Load command line arguments
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Recherche de références"
    )
    parser.add_argument("toml_filename")
    args: argparse.Namespace = parser.parse_args()

    # Load search parameters from toml file
    toml_filename: Path = Path(args.toml_filename)
    data_dir: Path = toml_filename.parent
    toml_dict: dict = toml.load(toml_filename)

    # Define input/output Excel file names
    in_excel_file: Path = data_dir / Path(toml_dict["in_excel_file"])
    out_excel_file: Path = data_dir / (
        Path(
            f"{in_excel_file.stem}_publications_"
            f"{toml_dict['pub_year_first']}-{toml_dict['pub_year_last']}"
            f"{in_excel_file.suffix}"
        )
        if toml_dict["search_type"] == "Publications"
        else data_dir / Path(f"{in_excel_file.stem}_profils" f"{in_excel_file.suffix}")
    )

    # Define ReferenceQuery Class object containing the query parameters
    reference_query: ReferenceQuery = ReferenceQuery(
        data_dir=data_dir,
        in_excel_file=in_excel_file,
        in_excel_file_author_sheet=toml_dict["in_excel_file_author_sheet"],
        out_excel_file=out_excel_file,
        pub_year_first=toml_dict["pub_year_first"],
        pub_year_last=toml_dict["pub_year_last"],
        publication_types=toml_dict["publication_types"],
        local_affiliations=toml_dict["local_affiliations"],
        scopus_database_refresh_days=toml_dict.get("scopus_database_refresh_days", 0),
        uspto_patent_search=toml_dict.get("uspto_patent_search", True),
        espacenet_patent_search=toml_dict.get("espacenet_patent_search", True),
        espacenet_patent_search_results_file=toml_dict.get(
            "espacenet_patent_search_results_file", ""
        ),
    )

    # Run the bibliographic search!
    run_reference_search(
        reference_query=reference_query,
        search_type=toml_dict["search_type"],
    )


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"Temps d'exécution: {str(timedelta(seconds=int(time.time() - start_time)))}")
