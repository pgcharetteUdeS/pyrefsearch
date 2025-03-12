"""search_inpadoc.py

    Search the INPADOC worldwide patent library via espacenet

    The script uses the "patent_client" package for searches in the INPADOC database,
    see https://patent-client.readthedocs.io/en/latest/index.html.

    NB: An API key is required to access INPADOC ("International Patent Documentation"
        database of patent information maintained by the European Patent Office,
        accessible via espacent), see pyrefsearch.toml.

"""

__all__ = ["query_espacenet_patents_and_applications"]

import ast
import datetime
from datetime import timedelta
import pandas as pd
from patent_client import Inpadoc
from pathlib import Path
import re
import sys
import time

from referencequery import ReferenceQuery
from utils import console, tabulate_patents_per_author, to_lower_no_accents_no_hyphens


def _extract_patent_family_members(member_info) -> tuple[list, list]:
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
    console.print(
        f"Recherche dans espacenet des {len(reference_query.au_names)} inventeurs..."
    )
    for name in reference_query.au_names:
        patent_families_raw = pd.concat(
            [
                patent_families_raw,
                _fetch_inpadoc_patent_families_by_author_name(
                    last_name=name[0], first_name=name[1]
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
    console.print(
        f"Analyze dans espacenet des {len(patent_families_raw.index)} familles de brevets..."
    )
    for i, row in patent_families_raw.iterrows():
        console.print(
            f"{row['family_id']} ({i}/{len(patent_families_raw.index)})", end=", "
        )
        if not hash(i) % 10 and hash(i) > 0:
            console.print("")
        member_info = Inpadoc.objects.get(row["patent_id"])
        if any("[CA]" in s for s in member_info.inventors_epodoc) and member_info.title:
            # Store tile, inventors, and applicants for this family
            families.append(member_info.family_id)
            titles.append(member_info.title)
            inventors.append(member_info.inventors_original)
            applicants.append(member_info.applicants_original)

            # Store patent member info for this family
            (
                family_member_patent_ids,
                family_member_publication_dates,
            ) = _extract_patent_family_members(member_info)
            patent_ids.append(family_member_patent_ids)
            publication_dates.append(family_member_publication_dates)
    console.print("")

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
        console.print(
            "[red]Impossible d'extraire la date du fichier de résultats de recherche"
            f" '{reference_query.espacenet_patent_search_results_file}' "
            "qui doit être en format '<filename>YYYYMMDD.xlsx'![/red]",
            soft_wrap=True,
        )
        sys.exit()

    file_date = datetime.datetime.strptime(match[1], "%Y%m%d").date()
    if datetime.date.today() - file_date >= timedelta(days=30):
        console.print(
            "[yellow]WARNING: Les données dans le fichier "
            f"'{reference_query.espacenet_patent_search_results_file}' "
            "ont plus de 30 jours![/yellow]",
            soft_wrap=True,
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
    patent_families: pd.DataFrame
    if reference_query.espacenet_patent_search_results_file:
        patent_families = _load_inpadoc_search_results_from_excel_file(reference_query)
    else:
        # else, search espacenet for patent families by author name, save to file
        patent_families = _search_espacenet_by_author_name(reference_query)

    # Add columns with local inventors and number of co-inventors to the dataframe
    local_inventors = patent_families["Inventeurs"].apply(
        lambda inventors: [
            name[0]
            for name in reference_query.au_names
            if any(
                (
                    to_lower_no_accents_no_hyphens(name[0])
                    in to_lower_no_accents_no_hyphens(inventor)
                )
                and (
                    to_lower_no_accents_no_hyphens(name[0])
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
    patent_application_counts_by_author: list = tabulate_patents_per_author(
        au_names=reference_query.au_names,
        au_ids=reference_query.au_ids,
        patents=applications_published_in_year_range,
    )
    patent_granted_counts_by_author: list = tabulate_patents_per_author(
        au_names=reference_query.au_names,
        au_ids=reference_query.au_ids,
        patents=patents_granted_in_year_range,
    )

    # Return dataframe for INPADOC patent applications and granted patents
    return (
        applications_published_in_year_range,
        patent_application_counts_by_author,
        patents_granted_in_year_range,
        patent_granted_counts_by_author,
    )
