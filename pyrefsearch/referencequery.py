"""referencequery.py

ReferenceQuery Class definition for storing reference query parameters

"""

__all__ = ["ReferenceQuery"]

import pandas as pd
from pathlib import Path
import re
from rich import print
import warnings

from utils import to_lower_no_accents_no_hyphens


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

    @staticmethod
    def show_3it_members_stats_on_console(authors: pd.DataFrame):
        n_members_women: int = len(authors[authors["Sexe"] == "F"])
        n_eng_members: int = len(authors[authors["Faculté / Service"] == "FGEN"])
        n_eng_members_regular_profs_only: int = len(
            authors[
                (authors["Faculté / Service"] == "FGEN")
                & (authors["Lien d'emploi UdeS"] == "Régulier")
            ]
        )
        n_members_with_office = len(authors[authors["Résidence"] != "Aucun bureau"])
        n_eng_members_regular_profs_with_office = len(
            authors[
                (authors["Faculté / Service"] == "FGEN")
                & (authors["Résidence"] != "Aucun bureau")
                & (authors["Lien d'emploi UdeS"] == "Régulier")
            ]
        )
        print(
            f"Membres réguliers du 3IT: {len(authors)} ({n_members_women / len(authors) * 100:.0f}% de femmes)"
        )
        print(
            "Membres réguliers qui ont un bureau au 3IT: "
            f"{n_members_with_office}/{len(authors)}"
        )
        print(
            f"Membres réguliers du 3IT en génie: {n_eng_members} "
            f"(Profs Réguliers: {n_eng_members_regular_profs_only}, "
            f"Profs Associés: {n_eng_members - n_eng_members_regular_profs_only})"
        )
        print(
            f"Profs réguliers en génie qui ont un bureau au 3IT: {n_eng_members_regular_profs_with_office}"
        )

    def __init__(
        self,
        data_dir: Path,
        in_excel_file: Path,
        in_excel_file_author_sheet: str,
        out_excel_file: Path,
        pub_year_first: int,
        pub_year_last: int,
        publication_types: list[str],
        local_affiliations: list[str],
        scopus_database_refresh_days: bool | int,
        uspto_patent_search: bool,
        espacenet_patent_search: bool,
        espacenet_patent_search_results_file: str,
    ):
        self.data_dir: Path = data_dir
        self.in_excel_file: Path = in_excel_file
        self.out_excel_file: Path = out_excel_file
        self.pub_year_first: int = pub_year_first
        self.pub_year_last: int = pub_year_last
        self.publication_types: list[str] = [row[0] for row in publication_types]
        self.publication_type_codes: list[str] = [row[1] for row in publication_types]
        self.local_affiliations: list[str] = [
            to_lower_no_accents_no_hyphens(s) for s in local_affiliations
        ]
        self.scopus_database_refresh_days: bool | int = scopus_database_refresh_days
        self.uspto_patent_search: bool = uspto_patent_search
        self.espacenet_patent_search: bool = espacenet_patent_search
        self.espacenet_patent_search_results_file: str = (
            espacenet_patent_search_results_file
        )
        print(f"Période de recherche: [{self.pub_year_first} - {self.pub_year_last}]")

        # Check input/output Excel file access, script fails if files already open
        self.check_excel_file_access(self.in_excel_file)
        self.check_excel_file_access(self.out_excel_file)

        # Load input Excel file into a dataframe, remove rows without author names
        warnings.simplefilter(action="ignore", category=UserWarning)
        input_data_full: pd.DataFrame = pd.read_excel(
            self.in_excel_file, sheet_name=in_excel_file_author_sheet
        )
        input_data_full = input_data_full.dropna(subset=["Nom"])

        # Strip any leading/trailing spaces in input data
        for series_name, series in input_data_full.items():
            input_data_full[series_name] = [str(s).strip() for s in series]

        # Extract author names from input Excel file, formatted either as a 3IT database
        # (author status tabulated by fiscal year) or as a simple list of names
        author_status_by_year_columns: list[str] = [
            f"{year}-{year + 1}"
            for year in range(self.pub_year_first, self.pub_year_last + 1)
        ]
        if all(col in input_data_full.columns for col in author_status_by_year_columns):
            # Author information is tabulated by fiscal year (XXXX-YYYY) and status (full
            # member or collaborator). Validate that the range of years specified
            # in the input data covers the range of years specified in the query,
            # filter by member status/year to remove collaborators.
            authors: pd.DataFrame = input_data_full.copy()[
                [
                    "Nom",
                    "Prénom",
                    "ID Scopus",
                    "Faculté / Service",
                    "Lien d'emploi UdeS",
                    "Résidence",
                    "Sexe",
                ]
                + author_status_by_year_columns
            ]
            authors["status"] = [
                "Régulier" if "Régulier" in yearly_status else "Collaborateur"
                for yearly_status in authors[
                    author_status_by_year_columns
                ].values.tolist()
            ]
            authors.drop(authors[authors.status == "Collaborateur"].index, inplace=True)
            self.show_3it_members_stats_on_console(authors)

        elif not any(
            # Author information is supplied as a simple list of names, no filtering
            re.search(r"\d{4}-\d{4}", column)
            for column in input_data_full.columns.tolist()
        ):
            authors: pd.DataFrame = input_data_full.copy()[
                ["Nom", "Prénom", "ID Scopus"]
            ]
            print(
                f"Nombre d'auteur.e.s dans le fichier '{self.in_excel_file}': {len(authors)}"
            )

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
