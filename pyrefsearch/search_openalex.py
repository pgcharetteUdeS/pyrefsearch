"""search_openalex.py

    Search OpenAlex database for author profiles and publications

"""

__all__ = [
    "query_openalex_author_profiles",
]

import pandas as pd
from pathlib import Path
from pyalex import Authors
from referencequery import ReferenceQuery
from utils import console


def query_openalex_author_profiles(reference_query: ReferenceQuery):
    console.print(
        "[green]\n** Recherche d'auteur.e.s dans la base de données OpenAlex **[/green]"
    )

    data_rows: list = []
    for name in reference_query.au_names:
        author_search_results = Authors().search(f"{name[1]} {name[0]}").get()
        if not author_search_results:
            console.print(
                f"ERREUR: Aucun résultat dans OpenAlex pour {name[1]} {name[0]}!"
            )
        data_rows.extend(
            [
                name[0],
                name[1],
                author["id"],
                author["works_count"],
                author["display_name"],
                author["created_date"],
                ", ".join(
                    [
                        last_inst["display_name"]
                        for last_inst in author["last_known_institutions"]
                    ]
                ),
                ", ".join([topic["display_name"] for topic in author["topics"]]),
            ]
            for author in author_search_results
        )
        data_rows.extend([""])
    df = pd.DataFrame(
        data_rows,
        columns=[
            "Surname",
            "Given name",
            "ID",
            "Works count",
            "Display name",
            "Created date",
            "Last known institutions",
            "Topics",
        ],
    )
    openalex_filename: Path = reference_query.data_dir / Path(
        f"{reference_query.in_excel_file.stem}"
        f"_{reference_query.pub_year_first}-{reference_query.pub_year_last}_OpenAlex.xlsx"
    )
    df.to_excel(openalex_filename, index=False)
    console.print(
        "Résultats de la recherche d'auteur.e.s dans OpenAlex sauvegardés "
        f"dans le fichier '{openalex_filename}'",
        soft_wrap=True,
    )
