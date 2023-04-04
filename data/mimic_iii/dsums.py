import itertools

import pandas as pd
from p_tqdm import p_uimap

from mimic_constants import PATH_TO_NOTES


def filter_valid_notes(hadm_df, dsum_date):
    dsum_date = pd.to_datetime(dsum_date)
    return list(filter(
        lambda record: pd.to_datetime(record['CHARTDATE']) <= dsum_date and record[
            'CATEGORY'] != 'Discharge summary',
        hadm_df.to_dict('records')))


if __name__ == '__main__':
    icu_df = pd.read_csv('/nlp/projects/summarization/kabupra/mimic/patients_time_outside_icu.csv')
    only_icu = icu_df[icu_df['total_outside_icu'] == 0]
    only_icu_hadm_ids = set(only_icu['hadm_id'].astype(str).unique().tolist())

    print(f'Loading notes from {PATH_TO_NOTES}')
    all_notes = pd.read_csv(PATH_TO_NOTES)
    all_notes = all_notes[all_notes['ISERROR'] != 1.0]
    all_notes.dropna(subset=['HADM_ID', 'CHARTDATE', 'TEXT'], inplace=True)
    all_notes['HADM_ID'] = all_notes['HADM_ID'].astype(int).astype(str)
    all_notes['CHARTDATE'] = pd.to_datetime(all_notes['CHARTDATE'])

    # Filtering for notes related to visits that took place solely in the ICU
    all_notes = all_notes[all_notes['HADM_ID'].isin(only_icu_hadm_ids)]

    dsum_df = all_notes[
        (all_notes['CATEGORY'] == 'Discharge summary') & (all_notes['DESCRIPTION'] == 'Report')
    ]

    def is_admission_note(note_description):
        return 'admi' in note_description.lower()

    admit_hadm_ids = set(all_notes[all_notes['DESCRIPTION'].apply(is_admission_note)]['HADM_ID'].unique())
    all_visits = len(all_notes['HADM_ID'].unique())
    print(f'{len(admit_hadm_ids)}/{all_visits} visits with an admission note')
    valid_hadm_ids = set(dsum_df['HADM_ID'].unique()).intersection(admit_hadm_ids)

    valid_dsums = dsum_df[dsum_df['HADM_ID'].isin(valid_hadm_ids)]
    hadm2date = dict(zip(valid_dsums['HADM_ID'], valid_dsums['CHARTDATE']))

    print(f'Filtering notes for {len(hadm2date)} unique HADM_IDs')
    valid_notes = all_notes[all_notes['HADM_ID'].isin(valid_hadm_ids)]
    notes_by_hadm = list(tuple(valid_notes.groupby('HADM_ID')))
    prior_notes = pd.DataFrame(list(itertools.chain(*list(p_uimap(
        lambda x: filter_valid_notes(x[1], hadm2date[x[0]]), notes_by_hadm)))))
    assert len(prior_notes) > 0  # no mismatch in HADM_ID formatting

    prior_notes_fn = 'source_notes.csv'
    dsums_fn = 'dsums.csv'
    print(f'Saving {len(prior_notes)} valid source notes prior to, or at, discharge to {prior_notes_fn}')
    prior_notes.to_csv(prior_notes_fn, index=False)

    print(f'Saving {len(valid_dsums)} to {dsums_fn}')
    valid_dsums.to_csv(dsums_fn, index=False)
