"""pyrefsearch.py

For a list of author names and range of dates supplied in an Excel file, publications in OpenAlex or Scopus,
patents in the INPADOC and USPTO databases.

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
from search_openalex import (
    query_author_profiles_by_id_openalex,
    query_author_homonyms_openalex,
    query_publications_openalex,
    config_openalex,
)
from search_scopus import (
    config_scopus,
    query_scopus_author_profiles_legacy,
    query_author_profiles_by_id_scopus,
    query_author_homonyms_scopus,
    query_publications_scopus,
)
from search_uspto import query_uspto_patents_and_applications
from utils import console
from version import __version__


def gen_power_shell_script_to_send_confirmation_emails(
    reference_query: ReferenceQuery,
) -> None:
    """
    Generate a Windows PowerShell script ("pyrefsearch_send_email_confirmation.ps1")
    to send confirmation email to people on a mailing list

    Args:
        reference_query (ReferenceQuery): Reference query object

    Returns: None

    """

    with open("shell_scripts\\pyrefsearch_send_email_confirmation.ps1", "w") as f:
        f.write("# Script to send confirmation emails to a list of recipients\n")
        f.write("# NB: the script is generated automatically by pyrefsearch.py\n\n")
        f.write(
            f'$Subject = "Recherche de publications dans {reference_query.publications_search_database}'
            f' pour les membres réguliers du 3IT du {reference_query.date_start} au {reference_query.date_end}"\n'
        )
        f.write("$currentDirectory = (Get-Location).Path\n\n")

        # Send logfile to Paul.Charette@Usehrbrooke.ca
        f.write('$logfilename = $currentDirectory + "\\pyrefsearch_last_month.log"\n')
        f.write(f"$attachments = @($logfilename)\n")
        f.write(
            '& ".\\shell_scripts\\send_email.ps1" -EmailTo '
            f'"{reference_query.previous_month_publications_search_confirmation_emails[0]}"'
            " -Subject $Subject -Body $Subject"
            " -Attachments $attachments\n\n"
        )

        # Send Excel results files to list of recipients
        f.write(
            '$recipients = "'
            + ",".join(
                reference_query.previous_month_publications_search_confirmation_emails
            )
            + '"\n'
        )
        f.write(
            f'$resultsfilename = $currentDirectory + "\\{str(reference_query.out_excel_file)}"\n'
        )
        f.write(f"$attachments = @($resultsfilename)\n")
        f.write(
            '& ".\\shell_scripts\\send_email.ps1" -EmailTo $recipients'
            " -Subject $Subject -Body $Subject"
            " -Attachments $attachments\n"
        )


def query_publications_and_patents(reference_query: ReferenceQuery) -> None:
    """
    Search for publications in OpenAlex/Scopus and patents in the USPTO & INPADOC databases

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info

    Returns: None

    """

    # Console banner
    console.print(
        "[green]\n** Période de recherche : "
        f"{reference_query.date_start} au {reference_query.date_end} **[/green]",
        soft_wrap=True,
    )
    if reference_query.previous_month_publications_search:
        console.print(
            "[yellow](Recherche pour le mois précédant)[/yellow]",
            soft_wrap=True,
        )

    # Search for publications either in the Scopus or OpenAlex databases
    publications_all: pd.DataFrame
    pub_type_counts_by_author: list[list[int | None]]
    author_homonyms: pd.DataFrame
    if reference_query.publications_search_database == "OpenAlex":
        # Init OpenAlex API
        config_openalex()

        # Fetch author profiles corresponding to user-supplied OpenAlex IDs, check they match
        # the user-supplied names, flag any inconsistencies in the "Erreurs" column
        console.print(
            "[green]\n** Recherche de profils d'auteurs dans OpenAlex **[/green]"
        )
        author_profiles = query_author_profiles_by_id_openalex(
            reference_query=reference_query
        )

        # Fetch publications, count publication types by author
        console.print("[green]\n** Recherche de publications dans OpenAlex **[/green]")
        publications, pub_type_counts_by_author = query_publications_openalex(
            reference_query=reference_query
        )

        # Fetch OpenAlex author profiles corresponding to user-supplied names, check for
        # author names with multiple OpenAlex IDs ("homonyms")
        console.print("[green]\n** Recherche d'homonymes dans OpenAlex **[/green]")
        author_homonyms = query_author_homonyms_openalex(
            reference_query=reference_query,
        )

    else:
        # Init Scopus API
        config_scopus()

        # Fetch author profiles corresponding to user-supplied Scopus IDs, check they match
        # the user-supplied names, flag any inconsistencies in the "Erreurs" column
        console.print(
            "[green]\n** Recherche de profils d'auteurs dans Scopus **[/green]"
        )
        author_profiles = query_author_profiles_by_id_scopus(
            reference_query=reference_query
        )

        # Fetch publications, count publication types by author
        console.print("[green]\n** Recherche de publications dans Scopus **[/green]")
        publications, pub_type_counts_by_author = query_publications_scopus(
            reference_query=reference_query
        )

        # Fetch Scopus author profiles corresponding to user-supplied names, check for
        # author names with multiple Scopus IDs ("homonyms"), load into dataframe
        author_homonyms = query_author_homonyms_scopus(
            reference_query=reference_query,
            homonyms_only=True,
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
        author_profiles["Brevets US (en instance)"] = (
            uspto_patent_application_counts_by_author
        )
        author_profiles["Brevets US (délivrés)"] = uspto_patent_counts_by_author

    # Fetch INPADOC applications and granted patents into separate dataframes, if required
    inpadoc_patent_applications = pd.DataFrame()
    inpadoc_patents = pd.DataFrame()
    if reference_query.espacenet_patent_search:
        console.print("[green]\n** Recherche de brevets dans espacenet **[/green]")
        (
            inpadoc_patent_applications,
            inpadoc_patent_application_counts_per_author,
            inpadoc_patents,
            inpadoc_patent_counts_per_author,
        ) = query_espacenet_patents_and_applications(reference_query)
        author_profiles["Brevets INPADOC (en instance)"] = (
            inpadoc_patent_application_counts_per_author
        )
        author_profiles["Brevets INPADOC (délivrés)"] = inpadoc_patent_counts_per_author
        console.print("Brevets INPADOC en instance: ", len(inpadoc_patent_applications))
        console.print("Brevets INPADOC délivrés: ", len(inpadoc_patents))

    # Write results to output Excel file
    console.print("[green]\n** Sauvegarde des résultats **[/green]")
    write_reference_query_results_to_excel_file(
        reference_query=reference_query,
        publications=publications,
        pub_type_counts_by_author=pub_type_counts_by_author,
        author_profiles=author_profiles,
        author_homonyms=author_homonyms,
        uspto_patents=uspto_patents,
        uspto_patent_applications=uspto_patent_applications,
        inpadoc_patents=inpadoc_patents,
        inpadoc_patent_applications=inpadoc_patent_applications,
    )

    # Write Windows Power Shell script to send confirmation emails in case of previous onth search
    if reference_query.previous_month_publications_search:
        gen_power_shell_script_to_send_confirmation_emails(
            reference_query=reference_query
        )


def pyrefsearch() -> None:
    # Console info starting messages
    python_version: str = (
        f"{str(sys.version_info.major)}"
        f".{str(sys.version_info.minor)}"
        f".{str(sys.version_info.micro)}"
    )
    console.print(
        f"{Path(__file__).stem} {__version__} " f"(running python {python_version})\n"
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

    # Load search type
    search_type: str = toml_dict.get("search_type", "Publications")

    # Assign the correct search codes depending on the database used
    publications_search_database: str = toml_dict.get(
        "publications_search_database", "OpenAlex"
    )
    publication_types: list[str]
    if publications_search_database == "Scopus":
        publication_types = toml_dict["publication_types_scopus"]
    else:
        publication_types = toml_dict["publication_types_openalex"]

    # If this a search for the previous, make sure OpenALex is used, else exit
    previous_month_publications_search: bool = toml_dict.get(
        "previous_month_publications_search", False
    )
    if (
        previous_month_publications_search
        and publications_search_database != "OpenAlex"
    ):
        console.print(
            "[red]ERREUR: OpenAlex doit être utilisé pour la recherche du mois précédant![/red]",
            soft_wrap=True,
        )
        sys.exit(0)

    # Determine search period dates
    date_start: date
    date_end: date
    if previous_month_publications_search:
        date_end = date.today()
        date_start = date_end - relativedelta(months=1)
    elif publications_search_database == "Scopus":
        date_start_scopus: date = toml_dict["date_start"]
        date_end_scopus: date = toml_dict["date_end"]
        date_start = date_start_scopus.replace(month=1, day=1)
        date_end = date_end_scopus.replace(month=12, day=31)
        if date_start_scopus != date_start or date_end_scopus != date_end:
            console.print(
                "[yellow]WARNING: 'La période de recherche dans Scopus"
                " ne peut être spécifiée qu'en années'[/yellow]",
                soft_wrap=True,
            )
    else:
        date_start = toml_dict["date_start"]
        date_end = toml_dict["date_end"]

    # Define ReferenceQuery Class object containing the query parameters
    reference_query: ReferenceQuery = ReferenceQuery(
        search_type=search_type,
        data_dir=str(toml_filename.parent),
        publications_search_database=publications_search_database,
        in_excel_file=toml_dict["in_excel_file"],
        in_excel_file_author_sheet=toml_dict["in_excel_file_author_sheet"],
        date_start=date_start,
        date_end=date_end,
        previous_month_publications_search=previous_month_publications_search,
        previous_month_publications_search_confirmation_emails=toml_dict.get(
            "previous_month_publications_search_confirmation_emails", []
        ),
        publication_types=publication_types,
        local_affiliations=toml_dict["local_affiliations"],
        scopus_database_refresh_days=toml_dict.get("scopus_database_refresh_days", 0),
        uspto_patent_search=toml_dict.get("uspto_patent_search", False),
        espacenet_patent_search=toml_dict.get("espacenet_patent_search", False),
        espacenet_max_retries=toml_dict.get("espacenet_max_retries", 25),
        espacenet_patent_search_results_file=toml_dict.get(
            "espacenet_patent_search_results_file", ""
        ),
    )

    # Run the query
    if search_type == "Publications":
        query_publications_and_patents(reference_query=reference_query)
    elif toml_dict["search_type"] == "Profils":
        query_scopus_author_profiles_legacy(reference_query=reference_query)
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
