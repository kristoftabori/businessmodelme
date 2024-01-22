from pathlib import Path
import pandas as pd
import numpy as np
import logging
import logging.config

resolved_path = Path("../configs/logging_config.ini").resolve()
logging.config.fileConfig(resolved_path)


class FeedbackSummary:
    """
    A class to summarize and analyze feedback from Google Sheets.

    This class takes dataframes of self and others' feedback and performs
    various analyses and transformations to summarize the feedback.

    Attributes
    ----------
    my_input_comments : str
        Column name for self-input comments.
    my_final_comments : str
        Column name for processed self-comments.
    others_input_comments : str
        Column name for others' input comments.
    others_final_comments : str
        Column name for processed others' comments.
    quality_name : str
        Column name for the quality being reviewed.
    count_name : str
        Column name for the count of others' comments.
    reviewer_name : str
        Column name for the reviewer's name.
    columns_from_others : list
        List of column names to include from others' dataframes.
    self_dataframe : pd.DataFrame
        DataFrame containing self-assessment data.
    others_dataframe : pd.DataFrame
        DataFrame containing others' assessment data.
    hierarchy : list
        List of columns dictating the hierarchy of data representation.
    _count_dataframe : pd.DataFrame or None
        Internal DataFrame for counting occurrences in others' feedback.
    _merged_dataframe : pd.DataFrame or None
        Internal DataFrame merging self and others' feedback.

    Methods
    -------
    count_dataframe():
        Property that returns a DataFrame counting occurrences in others'
        feedback.
    _merge_dataframes():
        Merges self and others' feedback into a single DataFrame.
    merged_dataframe():
        Property that returns the merged DataFrame of self and others'
        feedback.
    check_duplicates_in_self_assessment():
        Checks for duplicate entries in self-assessment.
    check_missing_qualities_from_self_assessment():
        Checks for any missing qualities in self-assessment compared to
        others' feedback.
    match_dataframe():
        Returns a DataFrame of qualities where self-assessment matches others'
        feedback.
    only_me_dataframe():
        Returns a DataFrame of qualities only mentioned in self-assessment.
    only_others_dataframe():
        Returns a DataFrame of qualities only mentioned in others' feedback.
    remove_redundancies(input_df: pd.DataFrame):
        Removes redundant entries in a DataFrame based on the hierarchy.
    """

    my_input_comments = "Comment"
    my_final_comments = "My Examples"
    others_input_comments = "Examples, so I can understand"
    others_final_comments = "Their Examples"
    quality_name = "Quality"
    count_name = "Others Count"
    reviewer_name = "Name"
    columns_from_others = [quality_name, others_input_comments, reviewer_name]

    def __init__(
        self,
        self_dataframe: pd.DataFrame,
        others_dataframe: pd.DataFrame,
        hierarchy: list
    ):
        """
        Initializes the FeedbackSummary with self and others' feedback data.

        Parameters
        ----------
        self_dataframe : pd.DataFrame
            DataFrame containing the self-assessment data.
        others_dataframe : pd.DataFrame
            DataFrame containing others' assessment data.
        hierarchy : list
            A list defining the order of columns for data representation.
        """
        self.logger = logging.getLogger(__name__)
        self.self_dataframe = self_dataframe
        self.others_dataframe = others_dataframe
        self.hierarchy = hierarchy
        self.check_duplicates_in_self_assessment()
        self.check_missing_qualities_from_self_assessment()
        self._count_dataframe = None
        self._merged_dataframe = None

    @property
    def count_dataframe(self) -> pd.DataFrame:
        """
        Returns a DataFrame counting occurrences in others' feedback.

        If not already created, this method computes a pivot table from
        the others_dataframe to count the occurrences of each quality.
        The resulting DataFrame is stored and returned for future use.

        Returns
        -------
        pd.DataFrame
            A DataFrame with counts of each quality based on others' feedback.
        """
        if self._count_dataframe is None:
            self._count_dataframe = pd.pivot_table(
                self.others_dataframe,
                values=[self.others_input_comments],
                index=[self.quality_name],
                aggfunc="count"
            ).reset_index().rename(
                columns={self.others_input_comments: self.count_name}
            ).astype({self.count_name: "int16"})
            self.logger.info("Adjectives counted for reviewers")
        return self._count_dataframe

    def _merge_dataframes(self) -> pd.DataFrame:
        """
        Merges the self and others' feedback data into a single DataFrame.

        This private method merges self_dataframe and others_dataframe into
        a single DataFrame. It combines the count of comments from others and
        the self comments, aligning them on the quality name.

        Returns
        -------
        pd.DataFrame
            The merged DataFrame containing both self and others' feedback.
        """
        tobereturned = pd.merge(
            (
                pd.merge(
                    self.self_dataframe,
                    self.count_dataframe,
                    how="left",
                    on=self.quality_name
                )
                .fillna(0.0)
                .astype({self.count_name: "int16"})
                .reset_index(drop=True)
                .rename(
                    columns={self.my_input_comments: self.my_final_comments}
                )
            ),
            self.others_dataframe[self.columns_from_others].rename(
                columns={
                    self.others_input_comments: self.others_final_comments
                }
            ),
            how="left",
            on=self.quality_name
        ).fillna("").sort_values(
            [self.count_name, self.quality_name, self.reviewer_name],
            ascending=[False, True, True]
        )

        return tobereturned[
            [col for col in tobereturned if col in self.hierarchy]
        ]

    @property
    def merged_dataframe(self) -> pd.DataFrame:
        """
        Returns the merged DataFrame of self and others' feedback.

        If not already created, this method calls _merge_dataframes
        to create a merged DataFrame. This DataFrame is then stored
        and returned for future use.

        Returns
        -------
        pd.DataFrame
            The merged DataFrame of self and others' feedback.
        """
        if self._merged_dataframe is None:
            self._merged_dataframe = self._merge_dataframes()
        return self._merged_dataframe

    def check_duplicates_in_self_assessment(self) -> None:
        """
        Checks for duplicates in the self-assessment data.

        This method identifies any duplicated entries in the self-assessment
        data based on the 'quality_name' column.

        Raises
        ------
        ValueError
            If duplicates are found in the self-assessment data.
        """
        quality_count = pd.pivot_table(
            self.self_dataframe,
            values=[self.my_input_comments],
            index=[self.quality_name],
            aggfunc="count"
        ).reset_index()
        duplicated_qualities = list(quality_count.loc[
            quality_count[self.my_input_comments] > 1,
            self.quality_name
        ])
        if len(duplicated_qualities) > 0:
            plural = "There are duplicated entries"
            condition = len(duplicated_qualities) > 1
            singular = "There is a duplicated entry"
            error_text_plural = plural if condition else singular
            q_str = ', '.join(duplicated_qualities)
            raise ValueError(
                f"{error_text_plural} found in the self-assessment: {q_str}"
            )

    def check_missing_qualities_from_self_assessment(self) -> None:
        """
        Checks for any missing qualities in the self-assessment data.

        This method identifies qualities that are present in others' feedback
        but missing from the self-assessment data.

        Raises
        ------
        ValueError
            If there are qualities missing in the self-assessment data.
        """
        others_qualities_count = pd.pivot_table(
            self.others_dataframe,
            values=[self.reviewer_name],
            index=[self.quality_name],
            aggfunc="count"
        ).reset_index()
        missing_qualities = list(
            others_qualities_count.loc[
                ~others_qualities_count[self.quality_name].isin(
                    self.self_dataframe[self.quality_name]
                ),
                self.quality_name
            ]
            .unique()
        )
        if len(missing_qualities) > 0:
            plural = "There are qualities missing from"
            condition = len(missing_qualities) > 1
            singular = "There is a quality missing from"
            error_text_plural = plural if condition else singular
            q_str = ', '.join(missing_qualities)
            raise ValueError(
                f"{error_text_plural} the self-assessment: {q_str}"
            )

    def match_dataframe(self) -> pd.DataFrame:
        """
        DataFrame of qualities where self-assessment matches others' feedback.

        This method identifies and returns the qualities that are mentioned
        both in self-assessment and in others' feedback.

        Returns
        -------
        pd.DataFrame
            A DataFrame of matching qualities.
        """
        tobereturned = self.merged_dataframe[
            (self.merged_dataframe[self.count_name] > 0)
            & (self.merged_dataframe[self.my_final_comments].str.len() > 0)
        ]
        match_num = len(list(tobereturned[self.quality_name].unique()))
        self.logger.info(f"{match_num} matching qualities found")
        for name in tobereturned[self.reviewer_name].unique():
            matching_adj_num = tobereturned.loc[
                tobereturned[self.reviewer_name] == name
            ].shape[0]
            if matching_adj_num > 0:
                all_adjectives = self.merged_dataframe.loc[
                    self.merged_dataframe[self.reviewer_name] == name
                ].shape[0]
                beginning_str = f"{name.title()} had {matching_adj_num}"
                all_num_str = f"(out of {all_adjectives})"
                end_str = f"{all_num_str} matching adjectives with you"
                self.logger.info(f"{beginning_str} {end_str}")
        return tobereturned

    def only_me_dataframe(self) -> pd.DataFrame:
        """
        Generates a DataFrame of qualities only mentioned in self-assessment.

        This method identifies and returns the qualities that are mentioned
        in self-assessment but not in others' feedback.

        Returns
        -------
        pd.DataFrame
            A DataFrame of qualities unique to self-assessment.
        """
        tobereturned = self.merged_dataframe[
            (self.merged_dataframe[self.count_name] == 0)
            & (self.merged_dataframe[self.my_final_comments].str.len() > 0)
        ]
        missing_num = len(list(tobereturned[self.quality_name].unique()))
        error_str = f"{missing_num} qualities found, which no one confirmed."
        self.logger.info(error_str)
        return tobereturned

    def only_others_dataframe(self) -> pd.DataFrame:
        """
        Generates a DataFrame of qualities only mentioned in others' feedback.

        This method identifies and returns the qualities that are mentioned
        in others' feedback but not in self-assessment.

        Returns
        -------
        pd.DataFrame
            A DataFrame of qualities unique to others' feedback.
        """
        tobereturned = self.merged_dataframe[
            (self.merged_dataframe[self.count_name] > 0)
            & (self.merged_dataframe[self.my_final_comments].str.len() == 0)
        ]
        missing_adjs = len(list(tobereturned[self.quality_name].unique()))
        error_end_str = "missing qualities found among others' feedbacks."
        self.logger.info(f"{missing_adjs} {error_end_str}")
        for name in tobereturned[self.reviewer_name].unique():
            missing_adj_num = tobereturned.loc[
                tobereturned[self.reviewer_name] == name
            ].shape[0]
            if missing_adj_num > 0:
                all_adjectives = self.merged_dataframe.loc[
                    self.merged_dataframe[self.reviewer_name] == name
                ].shape[0]
                beginning_str = f"{name.title()} had {missing_adj_num}"
                all_num_str = f"(out of {all_adjectives})"
                end_str = f"{all_num_str} adjectives which you didn't choose."
                self.logger.info(f"{beginning_str} {end_str}")
        return tobereturned

    def remove_redundancies(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes redundant entries in adjacent records to improve presentation.

        This method iterates through the DataFrame and removes redundant
        entries according to the defined hierarchy of columns.

        Parameters
        ----------
        input_df : pd.DataFrame
            The DataFrame from which to remove redundancies.

        Returns
        -------
        pd.DataFrame
            The DataFrame with redundancies removed.
        """
        tobereturned = input_df.copy()
        for ind, col in enumerate(self.hierarchy):
            condition = tobereturned.shift(1)[col] == tobereturned[col]
            for diff in range(ind):
                condition = condition & (
                    tobereturned[self.hierarchy[ind-diff-1]] == ""
                )
            tobereturned[col] = np.where(condition, "", tobereturned[col])
        return tobereturned
