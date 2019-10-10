import thulac
import json
import os
from os.path import join
from tqdm import tqdm
import random

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
        total_mentions = sum([v for k, v in mention_count])
        print(total_mentions)
        mention_count.sort(key=lambda x: x[1], reverse=True)
        mention_count = ['\t'.join([k, str(v)]) + '\n' for k, v in mention_count]

        w.writelines(mention_count)

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
    plt.hist(sent_length_ls, cumulative=True, range=(0, 200), bins=50)
    plt.ylabel('sent length count')
    plt.savefig(join('data','test.png'))


def test_thu():
    random.seed(42)
    data_dir = '/data/cleeag/THUCNews/财经'
    out_file = open('/data/cleeag/THUCNews/ner_test.txt', 'w')

    thu1 = thulac.thulac()
    # target_set = ('r', 's', 'n', 'np', 'ns', 'ni', 'nz', 'j')
    target_set = ('n', 'np', 'ns', 'ni', 'nz', 'j')
    count = 0
    for i, file in tqdm(enumerate(os.listdir(data_dir)), total=20):
        if random.uniform(0, 1) > 0.5:
            continue
        count += 1
        with open(join(data_dir, file), 'r') as r:
            text = r.read()
            out_file.write(text + '\n')

            snets = text.split('\n')
            for t in snets:
                tok_str = thu1.cut(t, text=True)
                tokens = tok_str.split()
                # print(tokens)
                cur = []
                out_toks = []
                for j in range(len(tokens)):
                    tok = tokens[j].split('_')
                    if tok[0].strip() == '' or len(tok) < 2:
                        continue
                    if tok[-1] not in target_set:
                        if len(cur) > 1:
                            # tok_ls, label_ls = [], []
                            # for tup in cur:
                            # print(cur)
                            tok_ls, label_ls = zip(*cur)
                            # print(tok_ls, label_ls)
                            out_file.write(''.join(tok_ls) + '\t' + '_'.join(label_ls) + '\n')
                        elif len(cur) == 1:
                            tok_ls, label_ls = zip(*cur)
                            out_file.write(''.join(tok_ls) + '\t' * 10 + '_'.join(label_ls) + '\n')
                        # out_toks.extend(cur)
                        # print(tok)
                        out_file.write(tok[0] + '\t'*10 + tok[1] + '\n')
                        cur = []
                        # out_file.write(tok[0] + '\t'*10 + tok[1] + '\n')
                    else:
                        # if tokens[i-1] in target_set:
                        # cur.append(tok[0] + '\t' + tok[1] + '\n')
                        cur.append((tok[0], tok[1]))
                        if  j == len(tokens) -1:
                            tok_ls, label_ls = zip(*cur)
                            out_file.write(''.join(tok_ls) + '\t' + '_'.join(label_ls) + '\n')
                        # out_file.write(tok[0] + '\t' + tok[1] + '\n')

                out_file.write('\n')
        if count > 20:
            break
    out_file.close()
