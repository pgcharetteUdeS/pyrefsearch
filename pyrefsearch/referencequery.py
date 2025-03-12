"""referencequery.py

ReferenceQuery Class definition for storing reference query parameters

"""

__all__ = ["ReferenceQuery"]

import pandas as pd
from pathlib import Path
import re
import sys
import warnings

from utils import console, to_lower_no_accents_no_hyphens


class ReferenceQuery:
    """
    Class to store reference query parameters
    """

    def check_excel_file_access(self):
        # Check that input Excel file exists and can be read from
        if not self.in_excel_file.is_file():
            console.print(
                f"[red]Le fichier '{self.in_excel_file}' n'existe pas![/red]",
                soft_wrap=True,
            )
            sys.exit()
        else:
            try:
                with open(self.in_excel_file, "r"):
                    pass
            except IOError:
                console.print(
                    f"[red]Impossible d'ouvrir le fichier '{self.in_excel_file}', [/red]"
                    "[red]le fermer s'il est ouvert dans Excel![/red]",
                    soft_wrap=True,
                )
                sys.exit()

        # Check that output Excel file either doesn't exist or can be written to
        if self.out_excel_file.is_file():
            try:
                with open(self.out_excel_file, "a"):
                    pass
            except IOError:
                console.print(
                    f"[red]Impossible d'ouvrir le fichier '{self.out_excel_file}', [/red]"
                    "[red]le fermer s'il est ouvert dans Excel![/red]",
                    soft_wrap=True,
                )
                sys.exit()

    def write_3it_member_stats_to_file(self, authors: pd.DataFrame):
        n_members_women: int = len(authors[authors["Sexe"] == "F"])
        n_eng_members: int = len(authors[authors["Faculté / Service"] == "FGEN"])
        n_eng_members_regular_profs_only: int = len(
            authors[
                (authors["Faculté / Service"] == "FGEN")
                & (authors["Lien d'emploi UdeS"] == "Régulier")
            ]
        )
        (
            n_eng_members_regular_profs_only_ee,
            n_eng_members_regular_profs_only_me,
            n_eng_members_regular_profs_only_cb,
            n_eng_members_regular_profs_only_cv,
        ) = [
            len(
                authors[
                    (authors["Faculté / Service"] == "FGEN")
                    & (authors["Lien d'emploi UdeS"] == "Régulier")
                    & (authors["Département"] == d)
                ]
            )
            for d in ["DGEGI", "DGMEC", "Chimie et biotechnologie", "Génie Civil"]
        ]
        n_eng_members_regular_profs_only_all: int = (
            n_eng_members_regular_profs_only_ee
            + n_eng_members_regular_profs_only_me
            + n_eng_members_regular_profs_only_cb
            + n_eng_members_regular_profs_only_cv
        )
        if n_eng_members_regular_profs_only_all != n_eng_members_regular_profs_only:
            console.print(
                f"[red]WARNING: Le nombre de professeurs réguliers en génie membres du 3IT "
                f"({n_eng_members_regular_profs_only}) ne correspond pas à la somme des "
                f"professeurs réguliers par département ({n_eng_members_regular_profs_only_all})"
                ", des informations d'affiliation sont incorrectes dans le fichier"
                f" '{self.in_excel_file}'![/red]",
                soft_wrap=True,
            )
        n_members_with_office = len(authors[authors["Résidence"] != "Aucun bureau"])
        n_eng_members_regular_profs_with_office = len(
            authors[
                (authors["Faculté / Service"] == "FGEN")
                & (authors["Résidence"] != "Aucun bureau")
                & (authors["Lien d'emploi UdeS"] == "Régulier")
            ]
        )
        n_eng_members_asso_profs_with_office = len(
            authors[
                (authors["Faculté / Service"] == "FGEN")
                & (authors["Résidence"] != "Aucun bureau")
                & (authors["Lien d'emploi UdeS"] != "Régulier")
            ]
        )

        stats_filename: Path = self.data_dir / Path(
            f"{self.in_excel_file.stem}"
            f"_{self.pub_year_first}-{self.pub_year_last}_stats.txt"
        )
        with open(stats_filename, "w") as f:
            f.write(
                f"* Membres réguliers du 3IT: {len(authors)} ({n_members_women / len(authors) * 100:.0f}% de femmes)\n"
            )
            f.write(
                f"* Membres réguliers du 3IT qui ont un bureau au 3IT: {n_members_with_office}\n"
            )
            f.write(f"* Membres réguliers du 3IT en génie: {n_eng_members}\n")
            f.write(
                f"    o Profs réguliers: {n_eng_members_regular_profs_only}, "
                f"dont {n_eng_members_regular_profs_with_office} avec bureau\n"
            )
            f.write(f"      - GEGI: {n_eng_members_regular_profs_only_ee}\n")
            f.write(f"      - GM: {n_eng_members_regular_profs_only_me}\n")
            f.write(
                f"      - Chimie & biotech: {n_eng_members_regular_profs_only_cb}\n"
            )
            f.write(f"      - Civil: {n_eng_members_regular_profs_only_cv}\n")
            if n_eng_members_regular_profs_only_all != n_eng_members_regular_profs_only:
                f.write(
                    "      - Autres: "
                    f"{n_eng_members_regular_profs_only-n_eng_members_regular_profs_only_all}"
                    " (devrait être zéro!)\n"
                )
            f.write(
                f"    o Profs associés: {n_eng_members - n_eng_members_regular_profs_only},"
                f" dont {n_eng_members_asso_profs_with_office} avec bureau\n"
            )
            console.print(
                "Statistiques des membres réguliers du 3IT écrites dans le fichier "
                f"'{stats_filename}'",
                soft_wrap=True,
            )

    def extract_authors_from_df(self, input_data_full: pd.DataFrame) -> pd.DataFrame:
        author_status_by_year_columns: list[str] = [
            f"{year}-{year + 1}"
            for year in range(self.pub_year_first, self.pub_year_last + 1)
        ]
        authors: pd.DataFrame
        if all(col in input_data_full.columns for col in author_status_by_year_columns):
            # Author information is tabulated by fiscal year (XXXX-YYYY) and status (full
            # member or collaborator). Validate that the range of years specified
            # in the input data covers the range of years specified in the query,
            # filter by member status/year to remove collaborators.
            authors = input_data_full.copy()[
                [
                    "Nom",
                    "Prénom",
                    "ID Scopus",
                    "Faculté / Service",
                    "Lien d'emploi UdeS",
                    "Département",
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
            if authors.empty:
                console.print(
                    f"[red]Aucun membre régulier du 3IT n'a été trouvé dans le fichier '{self.in_excel_file}'[/red]"
                    f"[red] pour la période de recherche [{self.pub_year_first}-{self.pub_year_last}][/red]",
                    soft_wrap=True,
                )
                sys.exit()
            self.write_3it_member_stats_to_file(authors)

        elif not any(
            # Author information is supplied as a simple list of names, no filtering
            re.search(r"\d{4}-\d{4}", column)
            for column in input_data_full.columns.tolist()
        ):
            if len(input_data_full) == 0:
                console.print(
                    f"[red]Le fichier '{self.in_excel_file}' est vide![/red]",
                    soft_wrap=True,
                )
                sys.exit()
            authors = input_data_full.copy()[["Nom", "Prénom", "ID Scopus"]]

        else:
            console.print(
                f"[red]L'intervalle de recherche [{self.pub_year_first}-{self.pub_year_last}] [/red]"
                f"[red]dépasse l'étendue des données dans le fichier '{self.in_excel_file}'![/red]",
                soft_wrap=True,
            )
            sys.exit()

        return authors

    def __init__(
        self,
        search_type: str,
        data_dir: str,
        in_excel_file: str,
        in_excel_file_author_sheet: str,
        pub_year_first: int,
        pub_year_last: int,
        publication_types: list[str],
        local_affiliations: list[str],
        scopus_database_refresh_days: bool | int,
        uspto_patent_search: bool,
        espacenet_patent_search: bool,
        espacenet_patent_search_results_file: str,
    ):
        self.search_type = search_type
        self.data_dir: Path = Path(data_dir)
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

        # Check search range
        if self.pub_year_first > self.pub_year_last:
            console.print(
                f"[red]ERREUR: L'année de début de recherche ({self.pub_year_first}) "
                f"doit être antérieure à l'année de fin de recherche ({self.pub_year_last})![/red]",
                soft_wrap=True,
            )
            sys.exit()

        # Build input/output Excel filename Path objects, check for access
        self.in_excel_file: Path = self.data_dir / Path(in_excel_file)
        self.out_excel_file: Path = data_dir / (
            Path(
                f"{self.in_excel_file.stem}"
                f"_{self.pub_year_first}-{self.pub_year_last}"
                f"_publications{self.in_excel_file.suffix}"
            )
            if self.search_type == "Publications"
            else Path(
                f"{self.in_excel_file.stem}_profils" f"{self.in_excel_file.suffix}"
            )
        )
        self.check_excel_file_access()

        # Load input Excel file data , remove rows without author names
        warnings.simplefilter(action="ignore", category=UserWarning)
        input_data_full: pd.DataFrame = pd.read_excel(
            self.in_excel_file, sheet_name=in_excel_file_author_sheet
        )
        input_data_full = input_data_full.dropna(subset=["Nom"])

        # Strip any leading/trailing spaces in the input data strings
        for series_name, series in input_data_full.items():
            input_data_full[series_name] = [str(s).strip() for s in series]

        # Extract author names from the input data, formatted either as a 3IT database
        # (author status tabulated by fiscal year) or as a simple list of names
        authors = self.extract_authors_from_df(input_data_full)
        self.au_names: list = authors[["Nom", "Prénom"]].values.tolist()
        console.print(
            f"Nombre d'auteur.e.s dans le fichier '{self.in_excel_file}': {len(authors)}"
        )

        # Extract Scopus IDs from the input data, replace non-integer values with 0
        self.au_id_to_index: dict = {}
        self.au_ids: list[int] = []
        if "ID Scopus" in authors:
            for scopus_id in authors["ID Scopus"].values.tolist():
                try:
                    self.au_ids.append(int(scopus_id))
                except ValueError:
                    self.au_ids.append(0)
