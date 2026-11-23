# Replication Study Data-Processing Pipeline

A Python preprocessing workflow developed for replication datasets from an English-language animacy experiment. It applies a sequence of dataset-specific transformations to Excel exports and prepares processed files for subsequent statistical analyses.

## Workflow

The script organizes preprocessing into five stages:

1. **Participant filtering and identifiers** — applies completion/progress and age-related exclusion criteria, converts selected fields to numeric values, and assigns sequential participant IDs.
2. **Variable harmonization** — renames demographic fields and recodes selected categorical responses.
3. **Feature preparation** — creates columns for animate/inanimate interaction ratings and filler responses, and removes export metadata that is not needed for analysis.
4. **Derived measures** — calculates participant-level means, standard deviations, and response counts for animate and inanimate items, and derives filler scores.
5. **Open-text preprocessing** — applies dictionary-based spell checking to responses in `Q56`.

## Input data

The workflow expects one or more Excel files named with the `group*.xlsx` pattern. The source files must follow the questionnaire schema used in the original study, including fields such as `Progress`, `Q3`–`Q56`, and the relevant item columns.

The original participant-level data are not included in this repository.

## Requirements

- Python
- pandas
- NumPy
- openpyxl
- pyspellchecker

Install the external dependencies with:

```bash
pip install pandas numpy openpyxl pyspellchecker
```

## Run

Before execution, edit the file search pattern in `PROCESS()` so that it points to the directory containing the source workbooks. The current script uses a Windows-specific path pattern.

Then run:

```bash
python ReplicationProcessing_2022-12.py
```

## Outputs

For each input workbook, the script writes a corresponding file with the suffix `_processed.xlsx` in the same directory. The processing function also returns a dictionary of processed DataFrames.

## Scope and limitations

This is a study-specific pipeline, not a general-purpose Excel ingestion tool. Its exclusion thresholds, questionnaire column names, recoding rules, and scoring transformations reflect the original study design. Dictionary-based spelling corrections can alter the meaning of open-text responses, so corrected text should be reviewed before substantive linguistic analysis. The current word-count calculation is not exported as a separate summary table.
