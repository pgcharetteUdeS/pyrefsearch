"""search_openalex.py

    Search OpenAlex database for author profiles by name

    https://github.com/J535D165/pyalex

"""

__all__ = [
    "query_openalex_author_profiles_by_name",
]

import pandas as pd
from pyalex import Authors
from referencequery import ReferenceQuery
from utils import console


def query_openalex_author_profiles_by_name(
    reference_query: ReferenceQuery,
) -> pd.DataFrame:
    """
    Fetch author profiles from OpenAlex database

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
