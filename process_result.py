import re
import os
from os.path import join
import json
from tqdm import tqdm
from collections import Counter, defaultdict
import random
import csv
import pandas as pd

home_dir = '/home/cleeag/cufe'

format = {'label_types': '',
          'mention': '',
          'span': '',
          'mention_type': '',
          'sentence': '',
          'source': '',
          'sentence_id': '',
          'mention_id': '',
          'error':''
          }

raw_answer_format = {'FineTypeA0': '',
                     'FineTypeA1': '',
                     'FineTypeA2': '',
                     'FineTypeA3': '',
                     'FineTypeA4': '',
                     'FineTypeB0': '',
                     'FineTypeB1': '',
                     'FineTypeB2': '',
                     'FineTypeB3': '',
                     'FineTypeB4': '',
                     'NA0': '',
                     'NA1': '',
                     'NA2': '',
                     'NA3': '',
                     'NA4': '',
                     }


def result_to_mention_file():
    raw_result_path = join(home_dir, 'data', 'results', 'Batch_252016_batch_results.csv')
    processed_result_path = join(home_dir, 'data', 'results', 'processed_batch_results.txt')
    processed_result_file = open(processed_result_path, 'w')
    raw_result = pd.read_csv(raw_result_path)
    WorkerId = raw_result[['WorkerId']]
    raw_result = raw_result.loc[:, "Input.mention_id_1":"Answer.taskAnswers"]
    raw_result = pd.concat([WorkerId, raw_result], axis=1)
    label_type_set, pro_type_set = set(), set()
    error_count_dict = defaultdict(list)
    type_count_dict = defaultdict(int)
    ans_mention_dict = {}

    mention_id_dict_path = join(home_dir, 'data', 'processed', 'cufe_mention_id_dict.txt')
    with open(mention_id_dict_path, 'r') as r:
        mention_id_dict = json.loads(r.readline())

    for i in range(len(raw_result)):
        answers = json.loads(raw_result.loc[i, 'Answer.taskAnswers'])[0]
        for field in raw_answer_format:
            if field not in answers:
                answers[field] = ''
        for row in range(1, 6):
            mention_pro_or_nam = mention_id_dict[str(raw_result.loc[i, f"Input.mention_id_{row}"])]['mention_type']
            type_count_dict[mention_pro_or_nam] += 1

            worker = raw_result.loc[i, f"WorkerId"]
            labeled_types = [answers[f'FineTypeA{row - 1}'], answers[f'FineTypeB{row - 1}']]
            error = answers[f'NA{row - 1}']['on']
            label_type_set.update(labeled_types)
            mention_id = str(raw_result.loc[i, f"Input.mention_id_{row}"])

            if mention_pro_or_nam == 'PRO':
                pro_type_set.update(labeled_types)
            if error == True: error_count_dict[worker].append(mention_id)
            if mention_id not in ans_mention_dict:
                label_dict = {
                    worker:labeled_types
                }
                error_dict = {
                    worker: error
                }
                ans_mention_dict[mention_id] = {'label_types': label_dict,
                                                'mention': raw_result.loc[i, f"Input.m{row}"],
                                                'sentence': raw_result.loc[i, f"Input.s{row}"],
                                                'mention_id': mention_id,
                                                'error': error_dict
                                                }
            else:
                ans_mention_dict[mention_id]['label_types'][worker] = labeled_types
                ans_mention_dict[mention_id]['error'][worker] = error

    ans_mention = [v for k, v in ans_mention_dict.items()]
    label_stat = {
        'same': 0,
        'partially_same': 0,
        'different': 0
    }
    j = 0
    for i, ans in enumerate(ans_mention):
        error = [ans['error'][worker] for worker in ans['error']]
        if len(ans['label_types']) < 2 or (error[0] or error[1]):
            j += 1
            continue
        labels = [l for person in ans['label_types'] for l in ans['label_types'][person]]
        assert len(labels) == 4
        # print(labels)
        # if labels[0][0] in labels[1] and labels[0][1] in labels[1]:
        if len(set(labels)) == 2:
            label_stat['same'] += 1
            print(labels)
        # elif len(set(labels[0]) - set(labels[1])) == 1:
        elif len(set(labels)) == 3:
            label_stat['partially_same'] += 1
            print(f'partially same: {labels}')
        elif len(set(labels)) == 4:
            label_stat['different'] += 1

        processed_result_file.write(json.dumps(ans, ensure_ascii=False) + '\n')

    processed_result_file.close()

    all_err = []
    for worker, er in error_count_dict.items():
        all_err.append(er)
    same_err = set(all_err[0]).intersection(set(all_err[1]))
    error_count_dict = {k:len(v) for k, v in error_count_dict.items()}

    print(f'all label types: {label_type_set}')
    print(f'number of distict labels: {len(label_type_set)}')
    print(f'total error count {error_count_dict}')
    print(f'count of same errors {same_err}')
    print(f'named entity and pronouns count: {type_count_dict}')
    print(f'annotation result: {label_stat}')

    print(j)


if __name__ == '__main__':
    result_to_mention_file()