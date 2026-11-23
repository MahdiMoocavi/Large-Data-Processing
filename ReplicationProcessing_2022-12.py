"""
The following pre-processing script automates the processing of data from replication studies for the English Animacy Experiment.
The script includes 5 stages of processing. Each stage is defined as a function, plus a global function to apply the algorithms to each data file.
- Erasmus University Rotterdam - Mahdi Moosavi, Dec 2023
"""

# Libraries
import pandas as pd
import numpy as np
import argparse
import re
from pathlib import Path
from spellchecker import SpellChecker
from openpyxl import Workbook, load_workbook


## STAGE 1: (I) applying exclusion criteria ; (II) converting specific columns to numeric type; & (III) adding Subject ID.
def STAGE1(df):
    # The original exports contain a redundant second row. Guard the
    # operation so a short or already-filtered file does not raise IndexError.
    df = df.copy()
    if len(df.index) > 1:
        df = df.drop(df.index[1])

    # Converting string columns to numeric type
    df['Progress'] = pd.to_numeric(df['Progress'], errors='coerce')
    df['Q3'] = pd.to_numeric(df['Q3'], errors='coerce')

    # Removing rows (participants) based on exclusion conditions
    df = df[df['Progress'] > 99]
    df = df[df['Q3'] > 18]

    # Adding 'SubID' column and assigning sequential IDs (after having excluded participants)
    df['SubID'] = range(1, len(df) + 1)

    return df


## STAGE 2: (I) renaming columns & (II) renaming values.
def STAGE2(df):
    # Renaming variables
    renaming_dict = {
        'Q4': 'Gender',
        'Q5': 'Education',
        'Q6': 'Native Language',
    }
    df.rename(columns=renaming_dict, inplace=True)

    # Recoding column values
    recode_dict = {
        'Gender': {'Male': '1', 'Female': '2', 'I do not identify with one of the above categories': '3'},
        'Education': {'Other': '1', 'Compulsory education (e.g., primary school, high school, ...)': '2', 'Higher education (e.g., bachelor, master, Ph.D., ...)': '3'},
        'Native Language': {'Other': '1', 'English': '2'},
    }
    df.replace(recode_dict, inplace=True)

    return df


## STAGE 3: (I) adding new columns & (II) removing columns.
def STAGE3(df):
    # Initializing new variables
    new_cols = [
        "Animate (average interaction rating)", "Animate (SD)", "Animate (n)",
        "Inanimate (average interaction rating)", "Inanimate (SD)", "Inanimate (n)",
        "Filler score (correct)", "Filler score (incorrect)", "Filler score",
        "Words (pre-processed)", "Words (incorrectly spelled)", "Words (corrected spelling)",
    ]
    for col in new_cols:
        df[col] = np.nan

    # Removing columns
    rem_cols = [
        'StartDate', 'EndDate', 'Status', 'Progress', 'Duration (in seconds)',
        'Finished', 'RecordedDate', 'ResponseId', 'DistributionChannel', 'UserLanguage',
    ]
    df.drop(columns=rem_cols, inplace=True, errors='ignore')

    return df


## STAGE 4: (I) calculating interaction scores & (II) calculating filler scores.
def STAGE4(df):
    # Ensure columns Q36 to Q55 are numeric and only contain 0 and 1 values
    for col in df.columns:
        if col in ['Q' + str(i) for i in range(36, 56)]:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
            df[col] = np.where(df[col] > 1, 1, df[col])
            df[col] = np.where(df[col] < 0, 0, df[col])
    
    # Convert all columns before Q56 to numeric
    for col in df.columns:
        if col != 'Q56':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Adjustments to columns Q10 and Q11
    if 'Q11' in df.columns:
        Q11_value = 7
        df['Q11'] = np.where(df['Q11'] != 1, df['Q11'] - Q11_value, df['Q11'])

    # Calculating interaction scores (average, SD, & n)
    df["Animate (average interaction rating)"] = df.loc[:, ["Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17","Q18","Q19","Q20","Q21"]].mean(axis=1)
    df["Animate (SD)"] = df.loc[:, ["Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17","Q18","Q19","Q20","Q21"]].std(axis=1)
    df["Animate (n)"] = df.loc[:, ["Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17","Q18","Q19","Q20","Q21"]].count(axis=1)
    
    df["Inanimate (average interaction rating)"] = df.loc[:, ["Q22","Q23","Q24","Q25","Q26","Q27","Q28","Q29","Q30","Q31","Q32","Q33"]].mean(axis=1)
    df["Inanimate (SD)"] = df.loc[:, ["Q22","Q23","Q24","Q25","Q26","Q27","Q28","Q29","Q30","Q31","Q32","Q33"]].std(axis=1)
    df["Inanimate (n)"] = df.loc[:, ["Q22","Q23","Q24","Q25","Q26","Q27","Q28","Q29","Q30","Q31","Q32","Q33"]].count(axis=1)
    
    # Calculating Filler scores (correct & incorrect responses)
    df['Filler score (correct)'] = df['Q36'] + df['Q37'] + df['Q38'] + df['Q39'] + df['Q40'] + df['Q41'] + df['Q42'] + df['Q43'] + df['Q44'] + df['Q45'] + df['Q46']
    df['Filler score (incorrect)'] = df['Q47'] + df['Q48'] + df['Q49'] + df['Q50'] + df['Q51'] + df['Q52'] + df['Q53'] + df['Q54'] + df['Q55']
    
    df['Filler score (incorrect)'] = df['Filler score (incorrect)'] - 9
    df['Filler score'] = df['Filler score (correct)'] + df['Filler score (incorrect)']
    
    return df


## STAGE 5: spell checking and text normalization.
def STAGE5(df):
    """Normalize open-text responses and retain a transparent correction log."""
    spell = SpellChecker()

    if "Q56" not in df.columns:
        raise ValueError("Expected open-text response column 'Q56' was not found.")

    original_text = df["Q56"].fillna("").astype(str)
    token_lists = original_text.str.split()
    all_words = [word for words in token_lists for word in words]

    misspelled = spell.unknown(all_words)
    corrections = {
        word: (spell.correction(word) or word)
        for word in misspelled
    }

    def row_misspellings(text):
        return " | ".join(
            word for word in text.split()
            if word.lower() in {item.lower() for item in misspelled}
        )

    def correct_words(text):
        for wrong, correct in corrections.items():
            text = re.sub(
                rf"\\b{re.escape(wrong)}\\b",
                correct,
                text,
                flags=re.IGNORECASE,
            )
        return text

    corrected_text = original_text.apply(correct_words)

    # Populate the audit columns initialized in STAGE3.
    df["Words (pre-processed)"] = original_text.str.lower().str.split().str.join(" ")
    df["Words (incorrectly spelled)"] = original_text.apply(row_misspellings)
    df["Words (corrected spelling)"] = corrected_text
    df["Q56"] = corrected_text

    return df


def PROCESS(input_dir=".", output_dir=None, pattern="group*.xlsx"):
    """Process all matching Excel files and return their processed DataFrames.

    Parameters
    ----------
    input_dir : str or Path
        Directory containing source Excel exports.
    output_dir : str or Path or None
        Directory for processed workbooks. Defaults to input_dir.
    pattern : str
        Glob pattern for input files.
    """
    input_path = Path(input_dir).expanduser().resolve()
    output_path = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else input_path
    )

    if not input_path.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_path}")

    excel_files = sorted(input_path.glob(pattern))
    if not excel_files:
        raise FileNotFoundError(
            f"No Excel files matching '{pattern}' were found in {input_path}"
        )

    output_path.mkdir(parents=True, exist_ok=True)
    processed_dfs = {}

    for excel_file in excel_files:
        df = pd.read_excel(excel_file)

        required = {"Progress", "Q3", "Q56"}
        missing = required.difference(df.columns)
        if missing:
            raise ValueError(
                f"{excel_file.name} is missing required columns: "
                + ", ".join(sorted(missing))
            )

        df_stage1 = STAGE1(df.copy())
        df_stage2 = STAGE2(df_stage1.copy())
        df_stage3 = STAGE3(df_stage2.copy())
        df_stage4 = STAGE4(df_stage3.copy())
        df_stage5 = STAGE5(df_stage4.copy())

        output_file = output_path / f"{excel_file.stem}_processed.xlsx"
        df_stage5.to_excel(output_file, index=False)
        processed_dfs[excel_file.name] = df_stage5

    return processed_dfs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Preprocess replication-study Excel files."
    )
    parser.add_argument(
        "--input-dir",
        default=".",
        help="Directory containing group*.xlsx files (default: current directory).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for processed files (default: input directory).",
    )
    parser.add_argument(
        "--pattern",
        default="group*.xlsx",
        help="Input file pattern (default: group*.xlsx).",
    )
    args = parser.parse_args()
    PROCESS(input_dir=args.input_dir, output_dir=args.output_dir, pattern=args.pattern)
### Calling the GLOBAL function
PROCESS()
