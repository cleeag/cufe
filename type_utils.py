import json
import os
from os.path import join
from tqdm import tqdm
import random
import re
import csv
from collections import defaultdict
import string

data_dir = '/home/cleeag/cufe/data'
cpu3_data_dir = '/data/cleeag/wikidata'

def filter_ontology(ontology):
    country_list_path = join(data_dir, 'types', 'ontology', 'country_list.txt')
    with open(country_list_path, 'r') as r:
        country_ls = [x.strip().lower() for x in r.readlines()]

    filtered_type_ls = []
    bad_char_list = ['(', ')', '{', '}', 'wiki', ':', 'Category:', 'aa', 'beta-', 'metabol', '.', 'phosph', 'dna-', 'gluc', 'response to', 'united states']
    bad_word_list = ['of', 'name', 'acid', 'atp', 'dna', 'acute', 'german', 'protein']
    bad_word_list.extend(country_ls)
    total_count, bad_count = 0, 0
    for word in tqdm(ontology):
        bad_tp = False
        for ban_char in bad_char_list:
            if ban_char.lower() in word.lower():
                # print(ban_word, word)
                bad_tp = True
                break
        for ban_word in bad_word_list:
            if ban_word.lower() in [x.lower() for x in word.split(' ')]:
                # print(ban_word, word)
                bad_tp = True
                break

        if not bad_tp:
            if len(word.split(' ')) > 5 \
                    or word == '' \
                    or word[0] in '.?!_*&^%$#@-=+"\'\/,<[>]\\\|' \
                    or word.split(' ')[0] in list(string.ascii_lowercase) + ['the', 'a*', 'uci', 'uk', 'un'] \
                    or word.split('-')[0] in list(string.ascii_lowercase) + ['the', 'a*', 'alpha', 'atp', 'australian'] \
                    or any(char.isdigit() for char in word):
                # print(word)
                bad_tp = True

        if not bad_tp:
            for w in word.split(' '):
                if len(w) > 0 and w[0].isupper():
                    # print(tp[0])
                    bad_tp = True
                    break
        if not bad_tp:
            try:
                word.encode(encoding='utf-8').decode('ascii')
            except UnicodeDecodeError:
                # print(tp[0])
                bad_tp = True


        if not bad_tp:
            filtered_type_ls.append(word)
            total_count += 1
        else:
            bad_count += 1
        if total_count > 10000:
            # break
            pass
    filtered_type_ls.sort()
    print(total_count, bad_count)


    return filtered_type_ls


def get_type_set_from_wikidata():
    all_types_path = join(cpu3_data_dir, 'wikidata_types.txt')
    # processed_all_types_path = join(cpu3_data_dir, 'processed_wikidata_types.txt')
    processed_all_types_path = join(data_dir, 'types',  'ontology', 'processed_wikidata_types.txt')

    type_dict = defaultdict(int)
    with open(all_types_path, 'r') as r:
        data = csv.reader(r)
        next(data)
        for row in data:
            diff_types = row[0].split(';')
            for dt in diff_types:
                type_dict[dt] += int(row[1])
                # print(type_dict[dt])
    type_ls = [k for k, v in type_dict.items()]
    # type_ls.sort(key=lambda x:x[1], reverse=True)
    type_ls.sort()
    print(type_ls[:10])

    # filter the list

    # filtered_type_ls = filter_ontology(type_ls)
    with open(processed_all_types_path, 'w') as w:
        # writer = csv.writer(w)
        # writer.writerows(filtered_type_ls)
        w.writelines([x +'\n' for x in type_ls])


def get_ontology_from_ufet_types():
    all_types_path = join(data_dir, 'types', 'all_types.txt')
    all_types_ontology_path = join(data_dir, 'types', 'ontology', 'all_types_ontology.txt')
    all_types_ontology_js_path = join(data_dir, 'types', 'js', 'all_types_ontology.js')
    vocab = set()
    with open(all_types_path, 'r') as r:
        all_types_ls = [x.strip() for x in r.readlines()]
    for t in all_types_ls:
        vocab.add(t)
        tw = t.split('_')
        vocab.update(tw)
    vocab = list(vocab)
    vocab.sort()
    vocab = [x + '\n' for x in vocab]
    with open(all_types_ontology_path, 'w') as w, open(all_types_ontology_js_path, 'w') as w2:
        w.writelines(vocab)
        ontology_str = '"' + '","'.join([x.strip() for x in vocab]) + '"'
        w2.write(f"""var all_types=[{ontology_str}];""")


def get_ontology():
    great_noun_list_path = join(data_dir, 'types', 'ontology', 'the_great_noun_list.txt')
    wikidata_ontology_path = join(data_dir, 'types',  'ontology', 'processed_wikidata_types.txt')
    # ufet_ontology_path = join(data_dir, 'types',  'ontology', 'ufet_ontology.txt')
    # all_types_path = join(data_dir, 'types', 'all_types.txt')
    all_types_path = join(data_dir, 'types', 'crowd_types.txt')
    yago_types_path = join(data_dir, 'types', 'ontology', 'yago_types.txt')

    # filters
    country_list_path = join(data_dir, 'types', 'ontology', 'country_list.txt')
    yt_bad_words_path = join(data_dir, 'types', 'ontology', 'yt_bad_words.txt')

    ontology_output_path = join(data_dir, 'types', 'ontology', 'cufe_ontology.txt')
    with open(great_noun_list_path, 'r') as r1, open(wikidata_ontology_path, 'r') as r2, \
            open(all_types_path, 'r') as r3, open(yago_types_path, 'r') as r4, \
            open(country_list_path, 'r') as r5, open(yt_bad_words_path, 'r') as r6:
        great_noun_list = r1.readlines()
        # ufet_ontology = json.loads(r2.read())
        wikidata_ontology = r2.readlines()
        all_types = r3.readlines()
        yago_types = r4.readlines()
        country_set = set([x.strip().lower() for x in r5.readlines()])
        yt_bad_words = set([x.strip().lower() for x in r6.read().split(',')])

    great_noun_list = [' '.join(x.strip().split('-')) for x in great_noun_list]
    # all_types = [' '.join(x.strip().split('_')).lower() for x in all_types]
    all_types = [' '.join(x.strip().split('\t')[0].split('_')) for x in all_types]
    # ufet_ontology = [x for x in ufet_ontology]
    wikidata_ontology = [x.strip() for x in wikidata_ontology]
    new_yago_types = []

    def camel_case_split(identifier):
        matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
        return [m.group(0) for m in matches]
    for typ in yago_types:
        typ_str = typ.split('\t')[0]
        s = ''
        for c in typ_str:
            if c.isdigit():
                break
            s += c
        s = camel_case_split(s)
        new_yago_types.append(' '.join([x for x in s]))


    print(len(great_noun_list), len(wikidata_ontology), len(all_types), len(new_yago_types))
    print(great_noun_list[:10], wikidata_ontology[:10], all_types[:10], new_yago_types[:10])
    great_noun_list = [x.lower()  for x in great_noun_list]
    wikidata_ontology = [x.lower()  for x in wikidata_ontology]
    all_types = [x.lower()  for x in all_types]
    new_yago_types = [x.lower()  for x in new_yago_types]
    gs = set(great_noun_list)
    gs.update(wikidata_ontology + all_types + new_yago_types)

    ontology = [x.lower() + '\n' for x in gs]

    ontology = filter_ontology(ontology)
    ontology_js_path = join(data_dir, 'types', 'js', 'cufe_ontology.js')
    with open(ontology_output_path, 'w') as w, open(ontology_js_path, 'w') as w2:
        w.writelines(ontology)
        ontology_str = '"' + '","'.join([x.strip().replace('"', '') for x in ontology]) + '"'
        w2.write(f"""var ontology=[{ontology_str}];""")




def types_to_js():
    crowd_types = join(data_dir, 'types', 'crowd_types.txt')
    general_types = join(data_dir, 'types', 'general_types.txt')
    all_types = join(data_dir, 'types', 'all_types.txt')
    with open(crowd_types, 'r') as r1, open(general_types, 'r') as r2, open(all_types, 'r') as r3:
        crowd_types_ls = [x.strip().split('\t')[0] for x in r1.readlines()]
        general_types_ls = [x.strip().split('\t')[0] for x in r2.readlines()]
        all_types_ls = [x.strip() for x in r3.readlines()]
        # crowd_types_ls = [x.strip() for x in r1.readlines()]
        # general_types_ls = [x.strip() for x in r2.readlines()]

    crowd_types = join(data_dir, 'types', 'js', 'crowd_types.js')
    general_types = join(data_dir, 'types', 'js', 'general_types.js')
    all_types = join(data_dir, 'types', 'js', 'all_types.js')
    with open(crowd_types, 'w') as w1, open(general_types, 'w') as w2, open(all_types, 'w') as w3:
        crowd_str = '"' + '","'.join(crowd_types_ls) + '"'
        w1.write(f"""var crowd_types=[{crowd_str}];""")
        general_str = '"' + '","'.join(general_types_ls) + '"'
        w2.write(f"""var general_types=[{general_str}];""")
        all_str = '"' + '","'.join(all_types_ls) + '"'
        w3.write(f"""var all_types=[{all_str}];""")

def get_crowd_and_distant_types():
    crowd_mention = join(data_dir, 'types', 'crowd_mentions.json')
    crowd_types = join(data_dir, 'types', 'crowd_types.txt')
    types_en2cn = join(data_dir, 'types', 'types_en2cn.txt')
    distant_types = join(data_dir, 'types', 'distant_types.txt')
    with open(crowd_mention, 'r') as r:
        crowd_mention = r.readlines()
    with open(types_en2cn, 'r') as r:
        types_en2cn = r.readlines()
        types_en2cn_dict = {x.split()[0]: x.split()[1] for x in types_en2cn}
        # print(types_en2cn_dict)
    with open(crowd_types, 'w') as w1, open(distant_types, 'w') as w2:
        crowd_types_set = set()
        for line in crowd_mention:
            mention_dict = json.loads(line)
            types = mention_dict['y_str']
            crowd_types_set.update(types)

        # crowd_types = ['\t'.join([x, types_en2cn_dict[x]]) for x in crowd_types]
        crowd_types_ls = []
        for x in crowd_types_set:
            if x in types_en2cn_dict:
                crowd_types_ls.append('\t'.join([x, types_en2cn_dict[x]]))
        crowd_types_ls.sort()

        distant_types = set(types_en2cn) - set(crowd_types_ls)
        distant_types = list(distant_types)
        distant_types.sort()
        crowd_types_ls = [x + '\n' for x in crowd_types_ls]
        distant_types = [x + '\n'  for x in distant_types]
        w1.writelines(crowd_types_ls)
        w2.writelines(distant_types)


def find_duplicated():
    cn_types = join(data_dir, 'types', 'chinese_types.txt')

    with open(cn_types, 'r') as r1:
        cn_types = r1.readlines()
        had_set = set()
        count = 0
        for x in cn_types:
            if x.strip() in had_set:
                print(x)
                count += 1
            else: had_set.add(x.strip())
        print(count)

def generate_type_files():
    org_types = join(data_dir, 'types', 'all_types.txt')
    cn_types = join(data_dir, 'types', 'chinese_types.txt')
    comb_types = join(data_dir, 'types', 'types_en2cn.txt')
    general_types = join(data_dir, 'types', 'general_types.txt')
    figer_types = join(data_dir, 'types', 'figer_types.txt')
    ultra_fine_types = join(data_dir, 'types', 'ultra_fine_types.txt')
    with open(org_types, 'r') as r1, open(cn_types, 'r') as r2, open(comb_types, 'w') as w:
        org_types = r1.readlines()
        cn_types = r2.readlines()

        out_ls = ['\t'.join([y.strip() for y in x]) + '\n' for x in zip(org_types, cn_types)]

        w.writelines(out_ls)

    with open(comb_types, 'r') as r1,\
        open(general_types, 'w') as w1, open(figer_types, 'w') as w2, open(ultra_fine_types, 'w') as w3:
        comb_types = r1.readlines()
        w1.writelines(comb_types[:9])
        w2.writelines(comb_types[9:130])
        w3.writelines(comb_types[130:])




if __name__ == '__main__':
    # generate_type_files()
    # find_duplicated()
    # get_crowd_and_distant_types()
    # types_to_js()
    # get_ontology_from_ufet_types()
    # get_type_set_from_wikidata()
    get_ontology()
