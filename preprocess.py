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

def get_amz_csv_format(test_num=100):
    input_file = 'cufe_mention_sampled'
    cufe_mention_path = join(home_dir, 'data', 'processed', f'{input_file}.txt')
    sample_mention_output_path = join(home_dir, 'data', 'processed', f'{input_file}_amazon.csv')
    test_sample_mention_output_path = join(home_dir, 'data', 'processed', f'{input_file}_amazon_{test_num}_test.csv')
    headers = ['mention_id_1', 'mention_id_2', 'mention_id_3', 'mention_id_4', 'mention_id_5',
               's1', 's2', 's3', 's4', 's5',
               'm1', 'm2', 'm3', 'm4', 'm5']
    if test_num > 0:
        tw = open(test_sample_mention_output_path, 'w')
        test_writer = csv.writer(tw)
        test_writer.writerow(headers)
    test_count = 0

    with open(cufe_mention_path, 'r') as r, open(sample_mention_output_path, 'w') as w:
        data = r.readlines()
        random.shuffle(data)
        writer = csv.writer(w)
        writer.writerow(headers)

        input_id_ls, sent_ls, mention_ls = [], [], []
        for line in data:
            if len(input_id_ls) == 5:
                writer.writerow(input_id_ls + sent_ls + mention_ls)
                if test_num > 0 and test_count < test_num:
                    test_writer.writerow(input_id_ls + sent_ls + mention_ls)
                    test_count += 5
                elif test_num > 0 and test_count == test_num:
                    tw.close()

                input_id_ls, sent_ls, mention_ls = [], [], []

            mention_dict = json.loads(line)
            input_id_ls.append(str(mention_dict['mention_id']))

            left = mention_dict['sentence'][:mention_dict['span'][0]]
            mention = mention_dict['mention']
            right = mention_dict['sentence'][mention_dict['span'][1]:]
            sent = left + '<mark>' + mention + '</mark>' + right
            sent_ls.append(sent)
            mention_ls.append(mention)


def filter_data(data):
    filtered_data = []
    count = 0
    for line in data:
        mention_dict = json.loads(line)
        mention = mention_dict['mention']
        sentence = mention_dict['sentence']
        # sentence_id = mention_dict['sentence_id']
        # mention_type = mention_dict['mention_type']
        # source = mention_dict['source']
        if 'http://' in mention:
            # print(mention)
            continue
        if '@' in sentence:
            # print(sentence)
            continue
        if len(mention) / len(sentence) > 0.2 and len(sentence) < 15:
            # print(mention)
            # print(sentence)
            if random.uniform(0, 1) > 0.4:
                count += 1
                continue
        # if len(mention) == 2 and '某' in mention:
            # print(mention)
            # print(sentence)
            # count += 1
            # continue

        new_line = line.replace('\n', '')
        new_line = new_line.replace('\\n', '')
        filtered_data.append(new_line)
    print(count)
    return filtered_data

def sample_mention_file():
    cufe_mention_path = join(home_dir, 'data', 'processed', 'cufe_mentions_all.txt')
    sample_mention_output_path = join(home_dir, 'data', 'processed', 'cufe_mention_sampled.txt')
    with open(cufe_mention_path, 'r') as r, open(sample_mention_output_path, 'w') as w:
        data = r.readlines()
        sampled_mention_ls =[]
        mention_count_dict = defaultdict(int)
        sent_count_dict = defaultdict(int)
        type_count_dict = defaultdict(int)
        prnoun_count_dict = defaultdict(int)
        source_count = defaultdict(int)
        source_count_2 = defaultdict(int)
        max_data_from_source = 3200
        max_seq_length = 256
        total_num = 8000
        nam_num = total_num / 10 * 9
        pro_num = total_num / 10

        filtered_data = filter_data(data)

        for i, line in tqdm(enumerate(filtered_data)):
            mention_dict = json.loads(line)
            mention = mention_dict['mention']
            sentence_id = mention_dict['sentence_id']
            mention_type = mention_dict['mention_type']
            source = mention_dict['source']
            sentence = mention_dict['sentence']
            if mention_type == 'NAM' and mention_count_dict[mention] < 8 \
                    and sent_count_dict[sentence_id] < 10 and source_count[source] < max_data_from_source \
                    and len(sentence) < max_seq_length \
                    and type_count_dict[mention_type] < nam_num:

                sampled_mention_ls.append(mention_dict)

                type_count_dict[mention_type] += 1
                source_count[source] += 1
                source_count_2['_'.join([source, mention_type])] += 1
            mention_count_dict[mention] += 1
            sent_count_dict[sentence_id] += 1

        random.shuffle(filtered_data)
        for i, line in tqdm(enumerate(filtered_data)):
            mention_dict = json.loads(line)
            mention = mention_dict['mention']
            sentence_id = mention_dict['sentence_id']
            mention_type = mention_dict['mention_type']
            source = mention_dict['source']
            sentence = mention_dict['sentence']
            if mention_type == 'PRO' \
                    and sent_count_dict[sentence_id] < 5\
                    and source_count[source] < max_data_from_source \
                    and len(sentence) < max_seq_length \
                    and type_count_dict[mention_type] < pro_num:

                sampled_mention_ls.append(mention_dict)

                type_count_dict[mention_type] += 1
                prnoun_count_dict[mention] += 1
                source_count[source] += 1
                source_count_2['_'.join([source, mention_type])] += 1

            mention_count_dict[mention] += 1
            sent_count_dict[sentence_id] += 1
        print()
        print(type_count_dict)
        print(prnoun_count_dict)
        print(source_count)
        print(source_count_2)
        random.shuffle(sampled_mention_ls)
        for mention_dict in sampled_mention_ls:
            w.write(json.dumps(mention_dict, ensure_ascii=False) + '\n')

    # with open(sample_mention_output_path, 'r') as r:
    #     data = r.readlines()
    #     type_count_dict = defaultdict(int)
    #     for line in data:
    #         type_count_dict[json.loads(line)['mention_type']] += 1
    # print(type_count_dict)



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
        renmin.extend(golden_horse)
        renmin.extend(msra)
        renmin.extend(boson)
        data = renmin
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
    target_set = ('rr', 'n_nap', 'n_nh', 'nt', 'jd')
    too_much = ['nr', 'nrg', 'ns', 'nz']
    all_type_set = set()
    with open(file_path, 'r') as r, open(output_file_path, 'w', encoding='utf-8') as w:
        lines = r.readlines()
        tmp = []
        pronoun_count, pronoun_set = 0, set()
        typ_count = defaultdict(int)
        for line in tqdm(lines):
            if line.strip() == '' or line.split()[1].strip() in end_sent_symbols:
                if line.strip() != '' and line.split()[1].strip() in end_sent_symbols:
                    tmp.append((line.split()[1], line.split()[2]))
                sent = ''
                mentions = []
                for i, (token, typ) in enumerate(tmp):
                    if typ in target_set:
                        if typ == 'rr':
                            pronoun_count += 1
                            typ_count['PRO'] += 1
                            pronoun_set.add(token)
                        else:
                            typ_count['NAM'] += 1
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
    print(typ_count)
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
                if line.strip() != '' and line[0] in end_sent_symbols:
                    token, token_idx, typ = (line.split()[0][:-1], line.split()[0][-1], line.split()[-1])
                    tmp.append((token, token_idx, typ))
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
            sentences = re.split(r'([。！？])', text)

            # sentences = [text]
            # print(sentences)
            for sent in sentences:
                # print(sent)
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
    # preprocess_golden_horse()
    # preprocess_boson()
    # preprocess_msra()
    # process_renmin()
    # combine_all()
    # get_mention_file()
    sample_mention_file()
    get_amz_csv_format(test_num=200)