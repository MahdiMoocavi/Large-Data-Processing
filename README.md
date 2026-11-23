# Animacy Replication Data Pipeline

A Python preprocessing workflow developed for replication datasets from an English-language animacy experiment. It applies a sequence of study-specific transformations to Excel exports and prepares processed workbooks for subsequent statistical analyses.

## Workflow

1. **Participant filtering and identifiers** — applies completion/progress and age-related exclusion criteria, converts selected fields to numeric values, and assigns sequential participant IDs.
2. **Variable harmonization** — renames demographic fields and recodes selected categorical responses.
3. **Feature preparation** — creates columns for animate/inanimate interaction ratings and filler responses, and removes export metadata not needed for analysis.
4. **Derived measures** — calculates participant-level means, standard deviations, and response counts for animate and inanimate items, and derives filler scores.
5. **Open-text preprocessing** — applies dictionary-based spelling suggestions and retains original, flagged, and corrected text in separate audit columns.

## Input data

The workflow expects one or more Excel files matching group*.xlsx (or a custom pattern). The source files must follow the questionnaire schema used in the original study, including fields such as Progress, Q3–Q56, and the relevant item columns. The original participant-level data are not included.

## Requirements

Python, pandas, NumPy, openpyxl, and pyspellchecker.

```bash
pip install pandas numpy openpyxl pyspellchecker
```

## Run

From the repository directory:

```bash
python animacy_replication_pipeline.py --input-dir ./data
```

Optionally specify a separate output directory and input pattern:

```bash
python animacy_replication_pipeline.py --input-dir ./data --output-dir ./processed --pattern "group*.xlsx"
```

## Outputs

For each input workbook, the script writes a corresponding _processed.xlsx file. The processing function also returns a dictionary of processed DataFrames.

## Scope and limitations

This is a study-specific pipeline, not a general-purpose Excel ingestion tool. Its exclusion thresholds, questionnaire column names, recoding rules, and scoring transformations reflect the original study design. Dictionary-based spelling corrections are suggestions and should be reviewed before substantive linguistic analysis. The source data are required to execute the workflow end-to-end.
