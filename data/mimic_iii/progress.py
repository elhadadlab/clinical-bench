import os
import ujson

import pandas as pd

from mimic_constants import PATH_TO_NOTES


def filter_valid_notes(hadm_df, dsum_date):
    dsum_date = pd.to_datetime(dsum_date)
    return list(filter(
        lambda record: pd.to_datetime(record['CHARTDATE']) <= dsum_date and record[
            'CATEGORY'] != 'Discharge summary',
        hadm_df.to_dict('records')))


def clean(row):
    return {
        'ROW_ID': str(row['ROW_ID']),
        'SUBJECT_ID': str(row['SUBJECT_ID']),
        'HADM_ID': str(row['HADM_ID']),
        'CHARTDATE': str(row['CHARTDATE']),
        'CHARTTIME': str(row['CHARTTIME']),
        'CATEGORY': row['CATEGORY'],
        'DESCRIPTION': row['DESCRIPTION'],
        'TEXT': row['TEXT'],
    }


if __name__ == '__main__':
    print(f'Loading notes from {PATH_TO_NOTES}')
    all_notes = pd.read_csv(PATH_TO_NOTES)
    all_notes = all_notes[all_notes['ISERROR'] != 1.0]
    all_notes.dropna(subset=['SUBJECT_ID', 'HADM_ID', 'CHARTDATE', 'TEXT'], inplace=True)
    all_notes['HADM_ID'] = all_notes['HADM_ID'].astype(int).astype(str)
    all_notes['HADM_ID'] = all_notes['SUBJECT_ID'].astype(int).astype(str)
    all_notes['CHARTDATE'] = pd.to_datetime(all_notes['CHARTDATE'])

    progress = all_notes[all_notes['DESCRIPTION'].apply(lambda x: 'progress' in x.lower())]
    print(f'Progress notes represent {len(progress)}/{len(all_notes)}')

    progress.drop_duplicates(subset=['TEXT'], keep='first', inplace=True)

    # 50+ progress notes
    subject_ct = progress['SUBJECT_ID'].value_counts()
    frequent_subjects = set([
        subject_id for subject_id, ct in zip(subject_ct.index, subject_ct.tolist()) if ct >= 50
    ])

    frequent_progress = progress[progress['SUBJECT_ID'].isin(frequent_subjects)]
    by_subject = frequent_progress.groupby('SUBJECT_ID')

    os.makedirs('progress', exist_ok=True)
    n = len(by_subject)
    for subject_id, subject_df in by_subject:
        subject_df = subject_df.sort_values(by='CHARTDATE').reset_index(drop=True)

        subject_records = list(map(clean, subject_df.to_dict('records')))
        subject_json = '\n'.join(list(map(ujson.dumps, subject_records)))

        out_fn = os.path.join('progress', f'{subject_id}.jsonl')
        with open(out_fn, 'w') as fd:
            fd.write(subject_json)

    print(f'Saved {len(frequent_progress)} Progress Notes for {n} patients...')
