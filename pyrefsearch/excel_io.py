"""excel_io.py

Excel file I/O utilities

"""

__all__ = ["write_reference_query_results_to_excel"]

from openpyxl import load_workbook
import pandas as pd

from referencequery import ReferenceQuery
from utils import console


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
    publications_all: pd.DataFrame,
    pub_type_counts_by_author: list,
    uspto_patents: pd.DataFrame,
    uspto_patent_applications: pd.DataFrame,
    inpadoc_patents: pd.DataFrame,
    inpadoc_patent_applications: pd.DataFrame,
    author_profiles_by_ids: pd.DataFrame,
    author_profiles_by_name: pd.DataFrame,
    publications_diff: bool = False,
) -> None:
    """
    Write publications search results to the output Excel file

    Args:
        reference_query (ReferenceQuery): ReferenceQuery Class object containing query info
        publications_all (pd.DataFrame): dataFrames publications found in Scopus
        pub_type_counts_by_author (list): lists of publication type by author
        uspto_patents (pd.DataFrame): USPTO patent application search results
        uspto_patent_applications (pd.DataFrame): USPTO patent search results
        inpadoc_patents (pd.DataFrame): INPADOC patent search result
        inpadoc_patent_applications (pd.DataFrame): INPADOC patent search results
        author_profiles_by_ids (pd.DataFrame): author search results by ids
        author_profiles_by_name (pd.DataFrame): author search results by names
        publications_diff (bool): True of this a Scopus differential request

    Returns: None

    """

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
            console.print(f"{pub_type}: {len(df)}")

            # Add "pub_type" publication counts to the author profiles
            if len(df) > 0:
                author_profiles_by_ids[pub_type] = pub_counts

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
    out_excel_file = (
        reference_query.out_excel_file.with_stem(
            f"{reference_query.out_excel_file.stem}_diff"
        )
        if publications_diff
        else reference_query.out_excel_file
    )
    with pd.ExcelWriter(out_excel_file) as writer:
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

        if not publications_diff:
            # Write all Scopus search result to a simgle sheet
            publications_all.to_excel(
                writer,
                index=False,
                sheet_name="Scopus (résultats complets)",
                freeze_panes=(1, 1),
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
                writer,
                index=False,
                sheet_name="Auteurs - Homonymes",
                freeze_panes=(1, 1),
            )
    console.print(
        "Résultats de la recherche sauvegardés " f"dans le fichier '{out_excel_file}'",
        soft_wrap=True,
    )

    # Attempt to adjust column widths in the output Excel file to reasonable values.
    # The solution is a hack because the auto_size/bestFit properties in
    # openpyxl.worksheet.dimensions.ColumnDimension() don't seem to work and the actual
    # column width sizing in Excel is system-dependant and a bit of a black box.
    workbook = load_workbook(out_excel_file)
    col_width_max: int = 100
    for sheet_name in workbook.sheetnames:
        for i, col in enumerate(workbook[sheet_name].columns):
            # workbook[sheet_name].column_dimensions[col[0].column_letter].bestFit = True
            col_width: int = int(max(len(str(cell.value)) for cell in col) * 0.85)
            col_width_min: int = 18 if i == 0 else 10
            workbook[sheet_name].column_dimensions[col[0].column_letter].width = max(
                min(col_width_max, col_width), col_width_min
            )
    workbook.save(out_excel_file)
