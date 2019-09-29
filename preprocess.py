import re
import os
from os.path import join
import json
from tqdm import tqdm
from collections import Counter, defaultdict
import random
import csv

random.seed(42)

home_dir = '/home/cleeag/cufe'

end_sent_symbols = ['。', '?', '？', '！']

format = {'mention': '',
          'span': '',
          'mention_type': '',
          'sentence': '',
          'source': '',
          'sentence_id': '',
          'mention_id': ''}


def explore_short_sents():
    # mention_path = join(home_dir, 'data', 'cufe_sent2mention.txt')
    mention_path = join(home_dir, 'data', 'processed', 'cufe_mentions_all.txt')
    short_sents_output = join(home_dir, 'data', 'processed', 'cufe_mention_short_sents.txt')
    with open(mention_path, 'r') as r:
        cufe_mentions_all = [json.loads(x) for x in r.readlines()]
    sent_length_dict = {}
    sent_length_ls = []
    with open(short_sents_output, 'w') as w:
        for sent_dict in tqdm(cufe_mentions_all):
            if sent_dict['mention_type'] == 'PRO' and sent_dict['sentence_id'] not in sent_length_dict:
                sent_length_dict[sent_dict['sentence_id']] = len(sent_dict['sentence'])
                sent_length_ls.append(len(sent_dict['sentence']))
                if len(sent_dict['sentence']) < 50:
                    w.write(json.dumps(sent_dict, ensure_ascii=False) + '\n')



    mention_count = [(k, v) for k, v in Counter(sent_length_ls).items()]
    total_mentions = sum([v for k, v in mention_count])
    print(total_mentions)
    mention_count.sort(key=lambda x: x[0], reverse=True)
    mention_count = ['\t'.join([str(k), str(v)]) + '\n' for k, v in mention_count]

    with open(join(home_dir, 'data', 'sent_len.txt'), 'w') as w:
        w.writelines(mention_count)

    import matplotlib.pyplot as plt
    import numpy as np
    plt.hist(sent_length_ls, cumulative=True, range=(0, 200), bins=50)
    plt.ylabel('sent length count')
    plt.savefig(join('data','test.png'))


def get_amz_csv_format():
    input_file = 'cufe_mention_sampled'
    # input_file = 'cufe_mention_short_sents'
    cufe_mention_path = join(home_dir, 'data', 'processed', f'{input_file}.txt')
    sample_mention_output_path = join(home_dir, 'data', 'processed', f'{input_file}_amazon.csv')
    delimiter = ','
    # delimiter = '\t'
    with open(cufe_mention_path, 'r') as r, open(sample_mention_output_path, 'w') as w:
        data = r.readlines()
        random.shuffle(data)
        writer = csv.writer(w)
        writer.writerow(['mention_id_1', 'mention_id_2', 'mention_id_3', 'mention_id_4', 'mention_id_5',
                         's1', 's2', 's3', 's4', 's5',
                         'm1', 'm2', 'm3', 'm4', 'm5'])
        input_id_ls, sent_ls, mention_ls = [], [], []
        for line in data:
            if len(input_id_ls) == 5:
                writer.writerow(input_id_ls + sent_ls + mention_ls)

                input_id_ls, sent_ls, mention_ls = [], [], []

            mention_dict = json.loads(line)
            input_id_ls.append(str(mention_dict['mention_id']))

            left = mention_dict['sentence'][:mention_dict['span'][0]]
            mention = mention_dict['mention']
            right = mention_dict['sentence'][mention_dict['span'][1]:]
            sent = left + '<mark>' + mention + '</mark>' + right
            sent_ls.append(sent)
            mention_ls.append(mention)


def count_mentions():
    sample_mention_path = join(home_dir, 'data', 'processed', 'cufe_mention_sampled.txt')
    mention_count_output_path = join(home_dir, 'data', 'processed', 'mention_count.txt')
    with open(sample_mention_path, 'r') as r, open(mention_count_output_path, 'w') as w:
        data = r.readlines()
        all_mentions = []
        for line in data:
            sent = json.loads(line)
            all_mentions.append(sent['mention'].strip())

        # all_mentions.sort()
        mention_count = [(k, v) for k, v in Counter(all_mentions).items()]
        total_mentions = sum([v for k, v in mention_count if v > 10])
        print(total_mentions)
        mention_count.sort(key=lambda x: x[1], reverse=True)
        mention_count = ['\t'.join([k, str(v)]) + '\n' for k, v in mention_count]

        w.writelines(mention_count)

def sample_mention_file():
    cufe_mention_path = join(home_dir, 'data', 'processed', 'cufe_mentions_all.txt')
    sample_mention_output_path = join(home_dir, 'data', 'processed', 'cufe_mention_sampled.txt')
    with open(cufe_mention_path, 'r') as r, open(sample_mention_output_path, 'w') as w:
        data = r.readlines()
        random.shuffle(data)
        mention_count_dict = defaultdict(int)
        sent_count_dict = defaultdict(int)
        type_count_dict = defaultdict(int)
        prnoun_count_dict = defaultdict(int)
        source_count = defaultdict(int)
        max_data_from_source = 5000
        max_seq_length = 256
        for i, line in tqdm(enumerate(data)):
            # mention_dict = json.loads(line, encoding='utf-8')
            mention_dict = json.loads(line)
            mention = mention_dict['mention']
            sentence_id = mention_dict['sentence_id']
            mention_type = mention_dict['mention_type']
            source = mention_dict['source']
            # if source == 'golden_horse':
            #     w.write(json.dumps(mention_dict, ensure_ascii=False) + '\n')
            #     type_count_dict[mention_type] += 1
            #     source_count[source] += 1
            if mention_type == 'NAM' and mention_count_dict[mention] < 5 \
                    and sent_count_dict[sentence_id] < 10 and source_count[source] < max_data_from_source \
                    and len(mention_dict['sentence']) < max_seq_length:
                w.write(json.dumps(mention_dict, ensure_ascii=False) + '\n')
                type_count_dict[mention_type] += 1
                source_count[source] += 1

            elif mention_type == 'PRO' and mention_count_dict[mention] < 75 \
                    and sent_count_dict[sentence_id] < 5 and source_count[source] < max_data_from_source \
                    and len(mention_dict['sentence']) < max_seq_length:
                w.write(json.dumps(mention_dict, ensure_ascii=False) + '\n')
                type_count_dict[mention_type] += 1
                prnoun_count_dict[mention] += 1
                source_count[source] += 1

            mention_count_dict[mention] += 1
            sent_count_dict[sentence_id] += 1
        print()
        print(type_count_dict)
        print(prnoun_count_dict)
        print(source_count)
    with open(sample_mention_output_path, 'r') as r:
        data = r.readlines()
        type_count_dict = defaultdict(int)
        for line in data:
            type_count_dict[json.loads(line)['mention_type']] += 1
    print(type_count_dict)



def get_mention_file():
    cufe_sent2mention_path = join(home_dir, 'data', 'processed', 'cufe_sent2mention.txt')
    mention_output_path = join(home_dir, 'data', 'processed', 'cufe_mentions_all.txt')
    mention_id_dict_output_path = join(home_dir, 'data', 'processed', 'cufe_mention_id_dict.txt')
    source_count = defaultdict(int)
    mention_id_dict = {}
    with open(cufe_sent2mention_path, 'r') as r, open(mention_output_path, 'w') as w:
        data = r.readlines()
        mention_id = 0
        for i, line in tqdm(enumerate(data)):
            sent_dict = json.loads(line, encoding='utf-8')
            sent = sent_dict['sentence']
            mentions = sent_dict['mention_spans']
            for men in mentions:
                w.write(json.dumps({'mention': men[2],
                                    'span': (men[0], men[1]),
                                    'mention_type': men[3],
                                    'sentence': sent,
                                    'source': men[4],
                                    'sentence_id': i,
                                    'mention_id': mention_id},
                                   ensure_ascii=False) + '\n')
                mention_id_dict[mention_id] = {'mention': men[2],
                                               'span': (men[0], men[1]),
                                               'mention_type': men[3],
                                               'sentence': sent,
                                               'source': men[4],
                                               'sentence_id': i
                                               }
                mention_id += 1
                source_count[men[4]] += 1
    with open(mention_id_dict_output_path, 'w') as w:
        w.write(json.dumps(mention_id_dict, ensure_ascii=False))
    print(source_count)



def combine_all():
    msra_file_path = join(home_dir, 'data', 'raw', 'msra_processed.txt')
    gh_file_path = join(home_dir, 'data', 'raw', 'golden_horse_processed.txt')
    boson_file_path = join(home_dir, 'data', 'raw', 'boson_processed.txt')
    renmin_file_path = join(home_dir, 'data', 'raw', 'renmin_processed.txt')

    output_file_path = join(home_dir, 'data', 'processed', 'cufe_sent2mention.txt')
    with open(msra_file_path, 'r') as r1, open(gh_file_path, 'r') as r2, \
            open(boson_file_path, 'r') as r3, open(renmin_file_path, 'r') as r4, \
            open(output_file_path, 'w', encoding='utf-8') as w:
        msra = r1.readlines()
        golden_horse = r2.readlines()
        boson = r3.readlines()
        renmin = r4.readlines()
        msra.extend(golden_horse)
        msra.extend(boson)
        # renmin = random.choices(renmin, k=8000)
        msra.extend(renmin)
        data = msra
        # random.shuffle(data)
        named_count, pronoun_count = 0, 0
        for i, line in enumerate(data):
            sent_dict = json.loads(line, encoding='utf-8')
            sent = sent_dict['sentence']
            mentions = sent_dict['mention_spans']
            new_mention = []
            for x in mentions:
                x = x.copy()
                # print(x[3])
                if x[3][-3:] == 'NOM' or x[3] == 'time':
                    continue
                if x[3] == 'rr':
                    x[3] = 'PRO'
                    pronoun_count += 1
                else:
                    x[3] = 'NAM'
                    named_count += 1
                new_mention.append(x)
                # if x[4] == 'golden_horse': gh_count += 1

            # print(mentions)
            # print(new_mention)
            if len(sent) > 10:
                w.write(json.dumps({'sentence': sent.replace(',', '，'),
                                    'mention_spans': new_mention}, ensure_ascii=False) + '\n')
            # if i > 500: break
    print(named_count, pronoun_count)
    # print(gh_count)
    # w.writelines(data)


def process_renmin():
    file_path = join(home_dir, 'data', 'raw', 'renmin.txt')
    output_file_path = join(home_dir, 'data', 'raw', 'renmin_processed.txt')
    """
    all_types = ['Ag', 'Bg', 'Dg', 'Mg', 'Ng', 'Qg', 'Rg', 'Tg', 'Ug', 'Vg', 'a', 'ad', 'an', 'b', 'c', 'd', 'dc', 'df',
                 'e', 'f', 'h', 'i', 'ia', 'ib', 'id', 'in', 'iv', 'j', 'jb', 'jd', 'jn', 'jv', 'k', 'l', 'la', 'lb',
                 'ld', 'ln', 'lv', 'm', 'mq', 'n', 'n_nap', 'n_nh', 'nr', 'nrg', 'ns', 'nt', 'nx', 'nz', 'o', 'p', 'q',
                 'qb', 'qc', 'qd', 'qe', 'qj', 'ql', 'qr', 'qt', 'qv', 'qy!2', 'qz', 'qz!1', 'qz!2', 'r', 'rr', 'ryw',
                 'rz', 'rzw', 's', 't', 'tt', 'u', 'ud', 'ue', 'ui', 'ul', 'uo', 'us', 'uz', 'v', 'vd', 'vi', 'vl',
                 'vn', 'vq', 'vt', 'vu', 'vx', 'w', 'wd', 'wf', 'wj', 'wky', 'wkz', 'wm', 'wp', 'ws', 'wt', 'wu', 'ww',
                 'wyy', 'wyz', 'y', 'z']
    """

    # target_set = ('r', 's', 'n', 'np', 'ns', 'ni', 'nz', 'j')
    # target_set = ('rr', 'n_nap', 'n_nh', 'nr', 'nrg', 'ns', 'nt', 'nx', 'nz', 'jd')
    target_set = ('rr', 'n_nap', 'n_nh', 'nt', 'nx', 'jd')
    too_much = ['nr', 'nrg', 'ns', 'nz']
    all_type_set = set()
    with open(file_path, 'r') as r, open(output_file_path, 'w', encoding='utf-8') as w:
        lines = r.readlines()
        tmp = []
        pronoun_count, pronoun_set = 0, set()
        for line in tqdm(lines):
            if line.strip() == '' or line.split()[1].strip() in end_sent_symbols:
                sent = ''
                mentions = []
                for i, (token, typ) in enumerate(tmp):
                    if typ in target_set:
                        if typ == 'rr':
                            pronoun_count += 1
                            pronoun_set.add(token)
                        span = [0, 0, '', '', '']
                        span[0] = len(sent)
                        span[1] = len(sent) + len(token)
                        span[2] = token
                        span[3] = typ
                        span[4] = 'renmin'
                        mentions.append(span)
                    sent += token

                tmp = []
                if len(sent.strip()) > 10 and not len(mentions) == 0:
                    w.write(json.dumps({'sentence': sent, 'mention_spans': mentions}, ensure_ascii=False) + '\n')
                    # print(sent, mentions)
                    # break
            else:
                stuff = line.split()
                token = stuff[1]
                typ = stuff[2]
                tmp.append((token, typ))
                all_type_set.add(typ)

    all_type_set = list(all_type_set)
    all_type_set.sort()
    # print(all_type_set)

    pronoun_set = list(pronoun_set)
    pronoun_set.sort()
    # print(pronoun_set)
    print(pronoun_count)


def preprocess_msra():
    file_path = join(home_dir, 'data', 'raw', 'msra.txt')
    output_file_path = join(home_dir, 'data', 'raw', 'msra_processed.txt')

    with open(file_path, 'r') as r, open(output_file_path, 'w', encoding='utf-8') as w:
        sentences = r.readlines()
        for org_sent in sentences:
            mentions = []
            sent = ' ' + org_sent
            ent_pattern = r'( )(.*?)(/[a-z]{,2})'
            while re.search(ent_pattern, sent):
                match_obj = re.search(ent_pattern, sent)
                # print(sent, match_obj.group(2))
                if match_obj.group(3) == '/o':
                    sent = sent[:match_obj.start()] + match_obj.group(2) + sent[match_obj.end():]
                else:
                    mentions.append([match_obj.start(),
                                     match_obj.start() + len(match_obj.group(2)),
                                     match_obj.group(2),
                                     match_obj.group(3),
                                     'msra'])
                    assert len(mentions[-1][2]) == mentions[-1][1] - mentions[-1][0]
                    sent = sent[:match_obj.start()] + match_obj.group(2) + sent[match_obj.end():]
            # sent = sent.replace('\o', '')
            if len(sent.strip()) > 5 and len(mentions) > 0:
                # print(org_sent, sent, mentions)
                w.write(json.dumps({'sentence': sent, 'mention_spans': mentions}, ensure_ascii=False) + '\n')
            # break


def preprocess_golden_horse():
    file_path = join(home_dir, 'data', 'raw', 'golden_horse.txt')
    output_file_path = join(home_dir, 'data', 'raw', 'golden_horse_processed.txt')
    user_mention_output_file_path = join(home_dir, 'data', 'raw', 'golden_horse_processed_user_mention.txt')
    total_mention_count, user_mention_count = 0, 0
    total_sent_count, sent_count, at_count = 0, 0, 0
    with open(file_path, 'r') as r, open(output_file_path, 'w', encoding='utf-8') as w, \
            open(user_mention_output_file_path, 'w', encoding='utf-8') as w2:
        line = r.readline()
        tmp = []
        while line:
            # print(line)
            if line.strip() == '' or line[0] in end_sent_symbols:
            # if line.strip() == '':
                sent = ''
                mentions = []
                rolling = False
                span = [0, 0, '', '', '']
                for i, (token, token_idx, typ) in enumerate(tmp):
                    sent += token
                    if typ[0] != 'I' and rolling == True:
                        span[1] = i
                        assert len(span[2]) == span[1] - span[0]
                        mentions.append(span)
                        span = [0, 0, '', '', '']
                        rolling = False
                    if typ[0] == 'B':
                        rolling = True
                        span[0] = i
                        span[2] += token
                        span[3] += typ
                        span[4] = 'golden_horse'
                    elif typ[0] == 'I':
                        if not rolling:
                            rolling = True
                            span[0] = i
                            span[2] += token
                            span[3] += typ
                            span[4] = 'golden_horse'
                        else:
                            span[2] += token
                total_sent_count += 1
                # print(sent)
                # if not len(sent.strip()) < 5 and not len(mentions) == 0:
                #     if '@' in sent:
                #         at_count += 1
                if not len(sent.strip()) < 5 and not len(mentions) == 0 and not '@' in sent:
                    user_mentions, clean_mention = [], []
                    for s in mentions:
                        if s[0] - 1 > 0 and sent[s[0]- 1] is '@':
                            user_mentions.append(s)
                        else:
                            clean_mention.append(s)
                    if len(user_mentions) > 0:
                        w2.write(json.dumps({'sentence': sent, 'mention_spans': user_mentions}, ensure_ascii=False) + '\n')
                        w.write(json.dumps({'sentence': sent, 'mention_spans': clean_mention}, ensure_ascii=False) + '\n')
                    else:
                        w.write(json.dumps({'sentence': sent, 'mention_spans': mentions}, ensure_ascii=False) + '\n')
                    total_mention_count += len(mentions) + len(clean_mention)
                    user_mention_count += len(user_mentions)
                    sent_count += 1
                    # print(sent, mentions)
                tmp = []
                line = r.readline()
                continue
                # break

            token, token_idx, typ = (line.split()[0][:-1], line.split()[0][-1], line.split()[-1])
            tmp.append((token, token_idx, typ))

            line = r.readline()
    print(total_mention_count, user_mention_count)
    print(total_sent_count, sent_count, at_count)


def preprocess_boson():
    mention_regex = r'\{\{(.*?)\:(.*?)\}\}'
    file_path = join(home_dir, 'data', 'raw', 'boson.txt')
    output_file_path = join(home_dir, 'data', 'raw', 'boson_processed.txt')
    with open(file_path, 'r') as r, open(output_file_path, 'w', encoding='utf-8') as w:
        data = r.readlines()
        # processed_data = []
        for _ in tqdm(data):
            text = _
            sentences = re.split(r'[。！？]', text)
            # sentences = [text]
            # print(sentences)
            for sent in sentences:
                sent = sent.strip()
                if len(sent) < 5:
                    continue
                spans = []
                while re.search(mention_regex, sent):
                    match_obj = re.search(mention_regex, sent)
                    sent = sent[:match_obj.start()] + match_obj.group(2) + sent[match_obj.end():]
                    span = (match_obj.start(),
                            match_obj.start() + len(match_obj.group(2)),
                            match_obj.group(2),
                            match_obj.group(1),
                            'boson'
                            )
                    assert len(span[2]) == span[1] - span[0]
                    spans.append(span)
                if len(spans) == 0:
                    continue
                # print(sent, spans)
                # processed_data.append({'sentence': sent, 'mention_spans': spans})
                w.write(json.dumps({'sentence': sent, 'mention_spans': spans}, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    preprocess_golden_horse()
    # preprocess_boson()
    # preprocess_msra()
    # process_renmin()
    combine_all()
    get_mention_file()
    sample_mention_file()
    count_mentions()
    # explore_short_sents()
    get_amz_csv_format()
#