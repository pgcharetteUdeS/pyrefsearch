"""pyrefsearch.py

    For a list of author names and range of years supplied in an Excel file, query:
    - references (publications in Scopus, patents in the INPADOC and USPTO databases)
      OR
    - author profiles (Scopus database), and write the results to an output Excel file.

    All execution parameters specified in the file "data/pyrefsearch.toml"

    Project on gitHub: https://github.com/pgcharetteUdeS/pyRefSearchUdeS

"""

import argparse
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
import sys
import time
import toml

from excel_io import write_reference_query_results_to_excel_file
from referencequery import ReferenceQuery
from search_espacenet import query_espacenet_patents_and_applications
from search_scopus import (
    scopus_init_api,
    query_scopus_author_profiles,
    query_scopus_author_profiles_by_id,
    query_scopus_author_profiles_by_name,
    query_scopus_publications,
)
from search_uspto import query_uspto_patents_and_applications
from utils import console
from version import __version__


def differential_scopus_search_results(
    reference_query: ReferenceQuery, publications_current: pd.DataFrame
) -> tuple[pd.DataFrame, Path]:
    """
    Return publications in publications_current that do not appear in publications_previous

    Args:
        reference_query (ReferenceQuery): Reference query
        publications_current (pd.DataFrame): dataframe of current publications

    Returns:
        publications_diff (pd.DataFrame), publications_previous_filename (Path)

    """

    # Load Scopus search results from the previous month (publications_previous)
    first_of_last_month = (date.today() - relativedelta(months=1)).replace(day=1)
    year_range: str = (
        f"{reference_query.pub_year_first-1}-{reference_query.pub_year_last-1}"
        if date.today().month == 1
        else f"{reference_query.pub_year_first}-{reference_query.pub_year_last}"
    )
    stem = reference_query.out_excel_file.stem
    publications_previous_filename = reference_query.out_excel_file.with_stem(
        f"{stem[:-len('_YYYY-YYYY_publications_YYYY-MM-DD')]}"
        f"_{year_range}_publications_{first_of_last_month}"
    )
    console.print(
        f"Fichier de référence: '{publications_previous_filename}'",
        soft_wrap=True,
    )
    with pd.ExcelFile(publications_previous_filename) as reader:
        publications_previous = pd.read_excel(
            reader, sheet_name="Scopus (résultats complets)"
        )

    # Publications in publications_current that do not appear in publications_previous
    publications_diff = publications_previous.merge(
        publications_current,
        on=["title"],
        how="right",
        suffixes=("_left", None),
        indicator=True,
    )
    publications_diff = publications_diff[publications_diff["_merge"] == "right_only"]
    publications_diff = publications_diff.loc[
        :, ~publications_diff.columns.str.endswith("_left")
    ]
    publications_diff.drop("_merge", axis=1, inplace=True)

    return publications_diff, publications_previous_filename


def gen_power_shell_script_to_send_confirmation_emails(
    reference_query: ReferenceQuery, out_excel_filename: Path
) -> None:
    """
    Generate a Windows PowerShell script ("pyrefsearch_send_email_confirmation.ps1")
    to send confirmation email to people on a mailing list

    Args:
        reference_query (ReferenceQuery): Reference query object
        out_excel_filename (Path): Path to the output Excel file

    Returns: None

    """

    date_from: str = str(out_excel_filename.stem)[-len("YYYY-MM-YY") :]
    date_to: str = str(out_excel_filename.stem)[
        -len("YYYY-MM-YY_DIFF_YYYY-MM-YY") : -len("_DIFF_YYYY-MM-YY")
    ]

    with open("pyrefsearch_send_email_confirmation.ps1", "w") as f:
        f.write("# Script to send confirmation emails to a list of recipients\n")
        f.write("# NB: the script is generated automatically by pyrefsearch.py\n\n")
        f.write(
            f'$Subject = "Résultats de la recherche Scopus du {date_from} au {date_to}"\n'
        )
        f.write("$currentDirectory = (Get-Location).Path\n\n")

        # Send logfile to Paul.Charette@Usehrbrooke.ca
        f.write('$logfilename = $currentDirectory + "\\pyrefsearch.log"\n')
        f.write(
            f'& ".\\send_email.ps1" -EmailTo "{reference_query.extract_scopus_diff_confirmation_emails[0]}"'
            " -Subject $Subject -Body $Subject"
            " -AttachmentFilename $logfilename\n\n"
        )

        # Send Excel results file to list of recipients
        f.write(
            '$recipients = "'
            + ",".join(reference_query.extract_scopus_diff_confirmation_emails)
            + '"\n'
        )
        f.write(
            f'$resultsfilename = $currentDirectory + "\\{str(out_excel_filename)}"\n'
        )
        f.write(
            '& ".\\send_email.ps1" -EmailTo $recipients'
            " -Subject $Subject -Body $Subject"
            " -AttachmentFilename $resultsfilename\n"
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
    console.print(
        "[green]Recherche de publications et brevets pour la période "
        f"{reference_query.pub_year_first}-{reference_query.pub_year_last}[/green]",
        soft_wrap=True,
    )

    # Init Scopus API
    scopus_init_api()

    # Fetch author profiles corresponding to user-supplied Scopus IDs, check they match
    # the user-supplied names, flag any inconsistencies in the "Erreurs" column
    console.print("[green]\n** Recherche de profils d'auteurs dans Scopus **[/green]")
    author_profiles_by_ids: pd.DataFrame = query_scopus_author_profiles_by_id(
        reference_query=reference_query
    )

    # Fetch publications by type in Scopus database, count publication types by author
    console.print("[green]\n** Recherche de publications dans Scopus **[/green]")
    publications_all: pd.DataFrame
    pub_type_counts_by_author: list[list[int | None]]
    publications_all, pub_type_counts_by_author = query_scopus_publications(
        reference_query=reference_query
    )

    # Fetch USPTO applications and granted patents into separate dataframes, if required
    uspto_patents: pd.DataFrame = pd.DataFrame()
    uspto_patent_applications: pd.DataFrame = pd.DataFrame()
    if reference_query.uspto_patent_search:
        console.print(
            "[green]\n** Recherche de brevets dans la base de données USPTO **[/green]"
        )
        uspto_patent_application_ids: list
        uspto_patent_counts_by_author: list
        uspto_patents, uspto_patent_application_ids, uspto_patent_counts_by_author = (
            query_uspto_patents_and_applications(
                reference_query=reference_query, applications=False
            )
        )
        console.print("Brevets US (délivrés): ", len(uspto_patents))
        uspto_patent_application_counts_by_author: list
        uspto_patent_applications, _, uspto_patent_application_counts_by_author = (
            query_uspto_patents_and_applications(
                reference_query=reference_query,
                applications=True,
                application_ids_to_remove=uspto_patent_application_ids,
            )
        )
        console.print("Brevets US (en instance): ", len(uspto_patent_applications))

        # Add patent application and published patent counts to the author profiles
        author_profiles_by_ids["Brevets US (en instance)"] = (
            uspto_patent_application_counts_by_author
        )
        author_profiles_by_ids["Brevets US (délivrés)"] = uspto_patent_counts_by_author

    # Fetch INPADOC applications and granted patents into separate dataframes, if required
    inpadoc_patent_applications = pd.DataFrame()
    inpadoc_patents = pd.DataFrame()
    if reference_query.espacenet_patent_search:
        console.print("[green]\n** Recherche brevets dans espacenet **[/green]")

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
        console.print("Brevets INPADOC en instance: ", len(inpadoc_patent_applications))
        console.print("Brevets INPADOC délivrés: ", len(inpadoc_patents))

    # Fetch Scopus author profiles corresponding to user-supplied names, check for
    # author names with multiple Scopus IDs ("homonyms"), load into dataframe
    author_profiles_by_name: pd.DataFrame = query_scopus_author_profiles_by_name(
        reference_query=reference_query,
        homonyms_only=True,
    )

    # Write results to output Excel file
    write_reference_query_results_to_excel_file(
        reference_query=reference_query,
        publications_all=publications_all,
        pub_type_counts_by_author=pub_type_counts_by_author,
        uspto_patents=uspto_patents,
        uspto_patent_applications=uspto_patent_applications,
        inpadoc_patents=inpadoc_patents,
        inpadoc_patent_applications=inpadoc_patent_applications,
        author_profiles_by_ids=author_profiles_by_ids,
        author_profiles_by_name=author_profiles_by_name,
    )

    # Differential Scopus publication search results relative to last month
    if reference_query.extract_scopus_diff:
        console.print(
            "[green]\n** Recherche différentielle de publications dans Scopus"
            " relativement au 1er du mois dernier **[/green]",
            soft_wrap=True,
        )
        publications_diff, publications_previous_filename = (
            differential_scopus_search_results(
                reference_query=reference_query, publications_current=publications_all
            )
        )
        out_excel_filename: Path = write_reference_query_results_to_excel_file(
            reference_query=reference_query,
            publications_all=publications_diff,
            pub_type_counts_by_author=pub_type_counts_by_author,
            uspto_patents=pd.DataFrame(),
            uspto_patent_applications=pd.DataFrame(),
            inpadoc_patents=pd.DataFrame(),
            inpadoc_patent_applications=pd.DataFrame(),
            author_profiles_by_ids=author_profiles_by_ids,
            author_profiles_by_name=author_profiles_by_name,
            publications_diff=True,
            publications_previous_filename=publications_previous_filename,
        )
        gen_power_shell_script_to_send_confirmation_emails(
            reference_query=reference_query, out_excel_filename=out_excel_filename
        )


def pyrefsearch() -> None:
    # Console info starting messages
    python_version: str = (
        f"{str(sys.version_info.major)}"
        f".{str(sys.version_info.minor)}"
        f".{str(sys.version_info.micro)}"
    )
    console.print(
        f"{Path(__file__).stem} {__version__} " f"(running python {python_version})"
    )

    # Load command line arguments
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Recherche de références"
    )
    parser.add_argument("toml_filename")
    parser.add_argument("--debug", action="store_true")
    args: argparse.Namespace = parser.parse_args()

    # Load the search parameters from the toml file
    toml_filename: Path = Path(args.toml_filename)
    toml_dict: dict = toml.load(toml_filename)

    # Define ReferenceQuery Class object containing the query parameters
    reference_query: ReferenceQuery = ReferenceQuery(
        search_type=toml_dict["search_type"],
        data_dir=str(toml_filename.parent),
        in_excel_file=toml_dict["in_excel_file"],
        in_excel_file_author_sheet=toml_dict["in_excel_file_author_sheet"],
        pub_year_first=toml_dict["pub_year_first"],
        pub_year_last=toml_dict["pub_year_last"],
        extract_scopus_diff=toml_dict.get("extract_scopus_diff", False),
        extract_scopus_diff_confirmation_emails=toml_dict.get(
            "extract_scopus_diff_confirmation_emails", []
        ),
        publication_types=toml_dict["publication_types"],
        local_affiliations=toml_dict["local_affiliations"],
        scopus_database_refresh_days=toml_dict.get("scopus_database_refresh_days", 0),
        uspto_patent_search=toml_dict.get("uspto_patent_search", True),
        espacenet_patent_search=toml_dict.get("espacenet_patent_search", True),
        espacenet_max_retries=toml_dict.get("espacenet_max_retries", 25),
        espacenet_patent_search_results_file=toml_dict.get(
            "espacenet_patent_search_results_file", ""
        ),
    )

    # Run the query
    if toml_dict["search_type"] == "Publications":
        query_publications_and_patents(reference_query=reference_query)
    elif toml_dict["search_type"] == "Profils":
        query_scopus_author_profiles(reference_query=reference_query)
    else:
        console.print(
            f"[red]ERREUR: '{toml_dict['search_type']}' est un type de recherche invalide, "
            "doit être 'Publications' ou 'Profils'[/red]",
            soft_wrap=True,
        )


if __name__ == "__main__":
    start_time = time.time()
    pyrefsearch()
    console.print(
        f"\nTemps d'exécution: {str(timedelta(seconds=int(time.time() - start_time)))}"
    )
