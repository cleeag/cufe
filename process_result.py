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



def merge_batches():
    batch_nums = ['3793008', '3790597']
    raw_result_paths = []
    output_path = join(home_dir, 'data', 'results', 'raw', f"Batch_{'_'.join(batch_nums)}_batch_results.csv")
    all_ans = []
    for i, bn in enumerate(batch_nums):
        raw_result_path = join(home_dir, 'data', 'results', 'raw', f'Batch_{bn}_batch_results.csv')
        raw_result_paths.append(join(home_dir, 'data', 'results', 'raw', f'Batch_{bn}_batch_results.csv'))
        with open(raw_result_path, 'r') as r:
            csv_reader = csv.reader(r)
            if i > 0:
                next(csv_reader)
            all_ans.extend([x for x in csv_reader])
    with open(output_path, 'w') as w:
        csv_writer = csv.writer(w)
        csv_writer.writerows(all_ans)

def ans_stats(ans_mention):
    print('generating stats for answers...')
    mention_id_dict_path = join(home_dir, 'data', 'processed', 'cufe_mention_id_dict.txt')
    with open(mention_id_dict_path, 'r') as r:
        mention_id_dict = json.loads(r.readline())

    label_type_set = set()
    pro_type_set = set()
    worker_ans_count = defaultdict(int)
    worker_to_error_dict = defaultdict(list)
    type_count_dict = defaultdict(int)

    for i, ans in enumerate(ans_mention):
        mention_pro_or_nam = mention_id_dict[ans['mention_id']]['mention_type']
        type_count_dict[mention_pro_or_nam] += 1

        label_type_set.update([v for k, vs in ans['label_types'].items() for v in vs])

        for worker in ans['label_types']:
            worker_ans_count[worker] += 1

        for worker in ans['error']:
            if ans['error'][worker] == True: worker_to_error_dict[worker].append(ans['mention_id'])

    error_count_dict = {k: len(v) for k, v in worker_to_error_dict.items()}
    err_set_ls = [set(x) for k, x in worker_to_error_dict.items()]
    same_err = set.intersection(*err_set_ls)

    # print(f'all label types: {label_type_set}')
    print(f'number of distict labels: {len(label_type_set)}')
    # print(f'worker to error: {worker_to_error_dict}')
    print(f'worker answer count: {worker_ans_count}')
    print(f'total workers: {len(worker_ans_count)}')
    print(f'total error count: {error_count_dict}')
    print(f'count of same errors: {same_err}')
    print(f'named entity and pronouns count: {type_count_dict}')



def result_to_mention_file():
    batch_num = '3793008_3790597'
    raw_result_path = join(home_dir, 'data', 'results', 'raw', f'Batch_{batch_num}_batch_results.csv')
    processed_result_path = join(home_dir, 'data', 'results', f'processed_{batch_num}_batch_results.txt')
    processed_result_no_worker_path = join(home_dir, 'data', 'results', f'processed_{batch_num}_batch_results_no_worker.txt')

    processed_result_file = open(processed_result_path, 'w')
    processed_result_no_worker = open(processed_result_no_worker_path, 'w')
    raw_result_all = pd.read_csv(raw_result_path)
    WorkerId = raw_result_all[['WorkerId']]
    AssignmentStatus = raw_result_all[['AssignmentStatus']]
    raw_result = raw_result_all.loc[:, "Input.mention_id_1":"Answer.taskAnswers"]
    raw_result = pd.concat([WorkerId, raw_result, AssignmentStatus], axis=1)

    ans_mention_dict = {}

    for i in range(len(raw_result)):
        if raw_result.loc[i, 'AssignmentStatus'] == 'Rejected':
            continue
        answers = json.loads(raw_result.loc[i, 'Answer.taskAnswers'])[0]
        for field in raw_answer_format:
            if field not in answers:
                answers[field] = ''
        for row in range(1, 6):

            worker = raw_result.loc[i, f"WorkerId"]
            labeled_types = [answers[f'FineTypeA{row - 1}'], answers[f'FineTypeB{row - 1}']]
            labeled_types = [x for x in labeled_types if len(x) != 0]
            error = answers[f'NA{row - 1}']['on']
            mention_id = str(raw_result.loc[i, f"Input.mention_id_{row}"])

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

    # get stats
    ans_stats(ans_mention)

    for i, ans in enumerate(ans_mention):

        processed_result_file.write(json.dumps(ans, ensure_ascii=False) + '\n')
        ans['label_types'] = list(set([v for k, vs in ans['label_types'].items() for v in vs]))
        processed_result_no_worker.write(json.dumps(ans, ensure_ascii=False) + '\n')

    processed_result_file.close()
    processed_result_no_worker.close()


if __name__ == '__main__':
    # merge_batches()
    result_to_mention_file()