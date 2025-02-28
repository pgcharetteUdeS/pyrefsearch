"""search_uspto.py

    Search USPTO database for patent applications or granted patents

    The script uses the "patent_client" package for searches in the USPTO databases,
    see https://patent-client.readthedocs.io/en/latest/index.html.

"""

__all__ = ["query_uspto_patents_and_applications"]

import pandas as pd
from patent_client import Patent, PublishedApplication
from rich import print
from unidecode import unidecode

from referencequery import ReferenceQuery
from utils import tabulate_patents_per_author, to_lower_no_accents_no_hyphens


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
        patent_counts_by_author = tabulate_patents_per_author(
            au_names=reference_query.au_names,
            au_ids=reference_query.au_ids,
            patents=patents,
        )

    return patents, application_ids, patent_counts_by_author
