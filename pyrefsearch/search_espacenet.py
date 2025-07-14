"""search_espacenet.py

    Search the espacenet patent database

    The script uses the "patent_client" package for searches,
    see https://patent-client.readthedocs.io/en/latest/index.html.

    NB: An API key is required, see pyrefsearch.toml.

"""

__all__ = ["query_espacenet_patents_and_applications"]

import pandas as pd
from patent_client import Inpadoc
import time

from excel_io import (
    load_espacenet_search_results_from_excel_file,
    write_espacenet_search_results_to_excel_file,
)
from referencequery import ReferenceQuery
from utils import console, tabulate_patents_per_author, to_lower_no_accents_no_hyphens


def _extract_patent_family_members(root_member_info) -> tuple[list, list]:
    # Start the list of member patent info for this family with the parent
    family_member_patent_ids: list = [root_member_info.application_number]
    family_member_publication_dates: list = [
        str(root_member_info.publication_reference_epodoc.date)
    ]

    # Add remaining family member patent info to the list
    for member in root_member_info.family:
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


def _extract_earliest_espacenet_patent_family_members(patent_families: pd.DataFrame):
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


def _fetch_espacenet_patent_families_by_author_name(
    reference_query: ReferenceQuery, last_name: str, first_name: str
) -> pd.DataFrame:
    """
    Fetch espacenet patent family IDs for author

    Args:
        reference_query (ReferenceQuery): Reference query object
        last_name (str): Last name of author
        first_name (str): First name of author

    Returns: DataFrame with unique espacenet patent family & patent IDs

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

    # Fetch parent record for the author
    retries: int = 0
    success: bool = False
    patents: pd.DataFrame = pd.DataFrame()
    while retries < reference_query.espacenet_max_retries and not success:
        try:
            patents = Inpadoc.objects.filter(cql_query=inventor_query_str()).to_pandas()
            success = True
        except Exception as e:
            retries += 1
            if retries == reference_query.espacenet_max_retries:
                console.print(
                    "[red]Erreur dans la recherche de brevets espacenet pour l'auteur "
                    f"{first_name} {last_name} ('{e}'): "
                    "cette erreur vient généralement du fait que la limite du nombre "
                    "d'accès pour une période donnée à la base de données a été excédée"
                    f" ({retries}) essais...[/red]"
                )
                console.print()
                exit()
            time.sleep(0.1)

    # Parse patents into a dataframe, retaining only "family_id" and "patent_id" columns
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
    Search the espacenet worldwide patent library by author name

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: DataFrame with patent family information

    """

    # Fetch unique patent families by author name
    patent_families_raw: pd.DataFrame = pd.DataFrame([])
    console.print(
        f"Recherche dans espacenet des {len(reference_query.au_names)} inventeurs",
        end="",
    )
    for name in reference_query.au_names:
        print(f" - {name[0]}", end="")
        patent_families_raw = pd.concat(
            [
                patent_families_raw,
                _fetch_espacenet_patent_families_by_author_name(
                    reference_query=reference_query,
                    last_name=name[0],
                    first_name=name[1],
                ),
            ],
            ignore_index=True,
        )
    print("")
    patent_families_raw = patent_families_raw.drop_duplicates(subset=["family_id"])
    patent_families_raw = patent_families_raw.reset_index(drop=True)

    # Loop to fetch patent family info
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
        # Fetch patent info from espacenet
        retries: int = 0
        success: bool = False
        patent_info: Inpadoc = Inpadoc()
        while retries < reference_query.espacenet_max_retries and not success:
            try:
                patent_info = Inpadoc.objects.get(row["patent_id"])
                success = True
            except Exception as e:
                retries += 1
                if retries == reference_query.espacenet_max_retries:
                    console.print(
                        f"\n[red]Erreur dans la recherche de brevets espacenet ('{e}'): "
                        "cette erreur vient généralement du fait que la limite du nombre "
                        "d'accès pour une période donnée à la base de données a été excédée"
                        f" ({retries} essais)...[/red]"
                    )
                    exit()
                time.sleep(0.1)

        console.print(
            f"{row['family_id']} ({hash(i)+1}/{len(patent_families_raw.index)}, "
            f"{retries} retries)",
            end=", ",
        )
        if not hash(i) % 6 and hash(i) > 0:
            console.print("")

        # Check that family contains at leat one Canadian inventor and title not empty
        if any("[CA]" in s for s in patent_info.inventors_epodoc) and patent_info.title:
            # Store tile, inventors, and applicants for this family
            families.append(patent_info.family_id)
            titles.append(patent_info.title)
            inventors.append(patent_info.inventors_original)
            applicants.append(patent_info.applicants_original)

            # Store patent member info for this family
            (
                family_member_patent_ids,
                family_member_publication_dates,
            ) = _extract_patent_family_members(root_member_info=patent_info)
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

    # Add earliest patent application and granted patent to the dataframe
    _extract_earliest_espacenet_patent_family_members(patent_families)

    # Sort family dataframe by title
    patent_families = patent_families.sort_values(by=["Titre"])

    # Write dataframe of all patent results to output Excel file
    write_espacenet_search_results_to_excel_file(
        reference_query=reference_query, patent_families=patent_families
    )

    # Return dataframe of search results
    return patent_families


def query_espacenet_patents_and_applications(
    reference_query: ReferenceQuery,
) -> tuple[pd.DataFrame, list, pd.DataFrame, list]:
    """

    Query the espacenet worldwide patent library.

    To connect to the espacenet worldwide patent search services, API keys are
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
        patent_families = load_espacenet_search_results_from_excel_file(reference_query)
        console.print(
            "Recherche espacenet dans le fichier "
            f"'{reference_query.espacenet_patent_search_results_file}'"
        )
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

    # Return dataframe for espacenet patent applications and granted patents
    return (
        applications_published_in_year_range,
        patent_application_counts_by_author,
        patents_granted_in_year_range,
        patent_granted_counts_by_author,
    )
