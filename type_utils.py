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


def camel_case_split(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]


def filter_ontology(ontology, chinese=False):
    if not chinese:
        print('filtering...')
    country_list_path = join(data_dir, 'types', 'filters', 'country_list.txt')
    nationality_list_path = join(data_dir, 'types', 'filters', 'nationality_list.csv')
    with open(country_list_path, 'r') as r, open(nationality_list_path, 'r') as r2:
        country_ls = [x.strip().lower() for x in r.readlines()]
        reader = csv.reader(r2)
        next(reader)
        nationality_ls = []
        for line in reader:
            country_ls.append(line[-2])
            nationality_ls.append(line[-1])

    filtered_type_ls = []
    bad_char_list = ['(', ')', '{', '}', 'wiki', ':', 'Category:', 'aa', 'beta-', 'metabol', '.',
                     'phosph', 'dna-', 'gluc', 'response to', 'united states', 'glu', 'gly']
    bad_word_list = ['of', 'name', 'acid', 'atp', 'dna', 'acute', 'german', 'protein',
                     'category', 'pronoun']
    bad_word_list.extend(country_ls)
    bad_word_list.extend(nationality_ls)
    total_count, bad_count = 0, 0
    # for word in tqdm(ontology, desc='filtering...'):
    for word in ontology:
        bad_tp = False

        for ban_char in bad_char_list:
            if ban_char.lower() in word.lower():
                # print(ban_char, word)
                bad_tp = True
                break
        for ban_word in bad_word_list:
            if ban_word.lower() in [x.lower().strip() for x in word.split(' ')]:
                # print(ban_word, word)
                bad_tp = True
                break

        if not bad_tp:
            if len(word.split(' ')) > 5 \
                    or word == '' \
                    or word[0] in '.?!_*&^%$#@-=+"\'\/,<[>]\\\|' \
                    or word.split(' ')[0].strip() in list(string.ascii_lowercase) + ['the', 'a*', 'uci', 'uk', 'un'] \
                    or word.split('-')[0].strip() in list(string.ascii_lowercase) + ['the', 'a*', 'alpha', 'atp',
                                                                                     'australian'] \
                    or any(char.isdigit() for char in word):
                # print(word)
                bad_tp = True

        if not bad_tp:
            for w in word.split(' '):
                if len(w) > 0 and w[0].isupper():
                    # print(w)
                    bad_tp = True
                    break
        if not bad_tp and not chinese:
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
    # print(total_count, bad_count)

    return filtered_type_ls

def write_ontology():
    ontology_path = join(data_dir, 'types', 'ontology', 'cufe_ontology.txt')
    ontology_output_path = join(data_dir, 'types', 'ontology', 'cufe_ontology2.txt')
    ontology_js_path = join(data_dir, 'types', 'on_web', 'cufe_ontology2.js')
    ontology_adding_path = join(data_dir, 'types', 'ontology', 'cufe_ontology_cn_trad.txt')
    with open(ontology_path, 'r') as r, open(ontology_adding_path, 'r') as r2:
        ontology = [x.strip() for x in r.readlines()]
        cn_adding = [x.strip() for x in r2.readlines()]
    ontology.extend(cn_adding)
    ontology = set(ontology)
    ontology = list(ontology)
    ontology.sort()
    with open(ontology_output_path, 'w') as w, open(ontology_js_path, 'w') as w2:
        w.writelines([x + '\n' for x in ontology])
        ontology_str = '"' + '","'.join([x.strip().replace('"', '') for x in ontology]) + '"'
        w2.write(f"""var ontology=[{ontology_str}];""")


def get_ontology(add_trad=True):
    # great_noun_list_path = join(data_dir, 'types', 'ontology', 'the_great_noun_list.txt')
    # ufet_ontology_path = join(data_dir, 'types',  'ontology', 'ufet_ontology.txt')
    # all_types_path = join(data_dir, 'types', 'all_types.txt')
    # yago_types_path = join(data_dir, 'types', 'ontology', 'yago_types.txt')
    wikidata_ontology_path = join(data_dir, 'types', 'ontology', 'processed_wikidata_types_>10.txt')
    crowd_types_path = join(data_dir, 'types', 'ontology', 'crowd_types_en2cn_revised.txt')

    # manually added
    with open(join(data_dir, 'types', 'ontology', 'manually_added.txt'), 'r') as r:
        manually_added = [x.strip() for x in r.readlines()]
    manually_added_set = [k for x in manually_added for k in x.strip().split('\t')]
    manually_added_en2cn_dict = {x.split('\t')[0].strip(): [x.split('\t')[1].strip()] for x in manually_added}

    with open(wikidata_ontology_path, 'r') as r2, \
            open(crowd_types_path, 'r') as r3:
        wikidata_ontology_org = r2.readlines()
        crowd_types_org = r3.readlines()

    # count the language in crowd types
    # for tup in crowd_types_org:
    #     crowd_en_set.add(tup.strip().split('\t')[0])
    #     crowd_cn_set.add(tup.strip().split('\t')[1])
    # lang_count_dict['en'] += len(crowd_en_set)
    # lang_count_dict['cn'] += len(crowd_cn_set)

    # great_noun_list = [' '.join(x.strip().split('-')) for x in great_noun_list]



    # new_yago_types = []

    # for typ in yago_types:
    #     typ_str = typ.split('\t')[0]
    #     s = ''
    #     for c in typ_str:
    #         if c.isdigit():
    #             break
    #         s += c
    #     s = camel_case_split(s)
    #     new_yago_types.append(' '.join([x for x in s]))

    # great_noun_list = [x.lower() for x in great_noun_list]
    # wikidata_ontology_cn = [v.strip() for d in wikidata_ontology for k, vs in d.items() for v in vs]
    # new_yago_types = [x.lower() for x in new_yago_types]

    # here split them into list of en and cn
    # crowd_types = [' '.join(v.split('_')).strip() for x in crowd_types_org for v in x.strip().split('\t')]
    crowd_types_en = [x.split('\t')[0].strip() for x in crowd_types_org]
    crowd_types_en2cn_dict = {x.split('\t')[0].strip(): [x.split('\t')[1].strip()] for x in crowd_types_org}

    wikidata_ontology_org = [json.loads(x.strip()) for x in wikidata_ontology_org]
    wikidata_ontology_en = [k.strip() for d in wikidata_ontology_org for k, v in d.items()]
    wikidata_ontology_en2cn_dict = {k.strip(): v for d in  wikidata_ontology_org for k, v in d.items()}

    en2cn_dict_all = defaultdict(list)
    for dictionary in [crowd_types_en2cn_dict, manually_added_en2cn_dict, wikidata_ontology_en2cn_dict]:
        for k, v in dictionary.items():
            en2cn_dict_all[k].extend(v)

    print(len(wikidata_ontology_org), len(crowd_types_org))
    print(wikidata_ontology_org[:10], crowd_types_org[:10])

    # here combine all sources and filter them
    gs = set(wikidata_ontology_en + crowd_types_en)

    ontology = filter_ontology(list(gs))
    print(len(ontology))
    ontology = list(ontology)
    lang_count_dict = defaultdict(int)
    cn_simplified_set = set()
    en2cn_dict_filtered = defaultdict(list)
    for en_word in tqdm(list(ontology), desc='adding chinese...'):
        if en_word in en2cn_dict_all:
            clean_cn = filter_ontology(en2cn_dict_all[en_word], chinese=True)
            if len(clean_cn) == 0:
                continue
            ontology.extend(clean_cn)
            en2cn_dict_filtered[en_word].extend(clean_cn)
            cn_simplified_set.update(clean_cn)
            lang_count_dict['en'] += 1
    lang_count_dict['cn'] = len(cn_simplified_set)

    print(len(ontology))
    ontology = set(ontology)
    print(len(ontology))

    if add_trad:
        ontology_adding_path = join(data_dir, 'types', 'ontology', 'cufe_ontology_cn_trad.txt')
        with open(ontology_adding_path, 'r') as r:
            cn_adding = [x.strip() for x in r.readlines()]
        ontology.update(cn_adding)
        print(len(cn_adding), len(set(cn_adding)), len(cn_simplified_set))
        # print(set(cn_adding) - cn_simplified_set)
    ontology.update(manually_added_set)
    ontology = list(ontology)
    print(len(ontology))
    print(lang_count_dict)
    ontology.sort()
    ontology = [x.lower() + '\n' for x in ontology]

    ontology_output_path = join(data_dir, 'types', 'ontology', 'cufe_ontology.txt')
    ontology_js_path = join(data_dir, 'types', 'on_web', 'cufe_ontology.js')
    with open(ontology_output_path, 'w') as w, open(ontology_js_path, 'w') as w2:
        w.writelines(ontology)
        print()
        ontology_str = '"' + '","'.join([x.strip().replace('"', '') for x in ontology]) + '"'
        w2.write(f"""var ontology=[{ontology_str}];""")


    # wikidata_ontology_tup_ls = [(k.strip(), one_d) for d in wikidata_ontology for k, v in d.items() for one_d in v]
    # crowd_types_tup_ls = [(x.split('\t')[0].strip(), x.split('\t')[1].strip()) for x in crowd_types_org]
    # en2cn_list = wikidata_ontology_tup_ls + crowd_types_tup_ls + manually_added_ls
    # all_en2cn_dict = {k:v for k, v in en2cn_list}

    # en2cn lists for reference
    # for en_word in ontology:
    #     if en_word.strip() in en2cn_dict_all:
    #         en2cn_dict_filtered[en_word.strip()] = en2cn_dict_all[en_word.strip()]
    for k, v in manually_added_en2cn_dict.items():
        en2cn_dict_filtered[k].extend(v)
    en2cn_list = [(k.strip(), one_d) for k, v in en2cn_dict_filtered.items() for one_d in v]
    en_ls = [k for k, v in en2cn_list]
    cn_ls = [v for k, v in en2cn_list]
    assert len(en_ls) == len(cn_ls)

    # en2cn_dict = defaultdict(set)
    # for tup in en2cn_list:
    #     en2cn_dict[tup[0]].add(tup[1])
    en2cn_dict_ls = [json.dumps([k, v], ensure_ascii=False) + '\n' for k, v in en2cn_dict_filtered.items()]

    en2cn_js_path = join(data_dir, 'types', 'on_web', 'en2cn_list.js')
    en2cn_27k_path = join(data_dir, 'types', 'on_web', 'en2cn_27k.txt')
    with open(en2cn_js_path, 'w') as w, open(en2cn_27k_path, 'w') as w2:
        en_str = '"' + '","'.join([x.strip().replace('"', '') for x in en_ls]) + '"'
        cn_str = '"' + '","'.join([x.strip().replace('"', '') for x in cn_ls]) + '"'
        w.write(f"""var en_l=[{en_str}];""")
        w.write(f"""var cn_l=[{cn_str}];""")
        w2.writelines(en2cn_dict_ls)

    cufe_ontology_en_path = join(data_dir, 'types', 'ontology', 'cufe_ontology_en.txt')
    cufe_ontology_cn_path = join(data_dir, 'types', 'ontology', 'cufe_ontology_cn.txt')
    with open(cufe_ontology_en_path, 'w') as w1, open(cufe_ontology_cn_path, 'w') as w2:
        en_tmp = [k.strip() + '\n' for k in en2cn_dict_filtered]
        en_tmp.sort()
        w1.writelines(en_tmp)
        en_tmp = list(set([v + '\n' for k, ls in en2cn_dict_filtered.items() for v in ls]))
        en_tmp.sort()
        w2.writelines(en_tmp)


def get_type_set_from_wikidata():
    return
    wikidata_types_path = join(cpu3_data_dir, 'wikidata_types.txt')
    processed_300k_types_path = join(cpu3_data_dir, 'processed_wikidata_types_300k.txt')
    processed_10_up_types_path = join(data_dir, 'types', 'ontology', 'processed_wikidata_types_>10.txt')
    wikidata_cn_path = join(cpu3_data_dir, 'wikidata.json')
    chinese_type_path = join(data_dir, 'types', 'chinese_types', 'wikidata_chinese_types_all_varients.txt')
    with open(wikidata_cn_path, 'r') as r:
        data = r.readlines()
    en_cn_dict = defaultdict(set)
    count = 0
    cn_types = set()
    for line in data:
        l_dict = json.loads(line)
        if 'label' in l_dict and 'en' in l_dict['label']:
            langs = ['zh', 'zh-hans', 'zh-hant', 'zh-cn', 'zh-hk']
            have = False
            for lang in langs:
                if lang in l_dict['label']:
                    en_cn_dict[l_dict['label']['en']].add(l_dict['label'][lang])
                    cn_types.add(l_dict['label'][lang])
                    have = True

            if not have:
                count += 1
    print(count)
    # return

    type_dict = defaultdict(int)
    with open(wikidata_types_path, 'r') as r:
        data = csv.reader(r)
        next(data)
        for row in data:
            diff_types = row[0].split(';')
            for dt in diff_types:
                if dt not in en_cn_dict: continue
                type_dict[dt] += int(row[1])
                # print(type_dict[dt])
    type_ls = [(k, en_cn_dict[k], v) for k, v in type_dict.items()]
    type_ls.sort(key=lambda x: x[2], reverse=True)
    type_ls_json = [json.dumps({'en': tup[0], 'cn': list(tup[1]), 'count':tup[2]}, ensure_ascii=False) + '\n'
                            for tup in type_ls ]
    type_ls_more_than_10 = [json.dumps({'en':tup[0], 'cn':list(tup[1])}, ensure_ascii=False) + '\n'
                            for tup in type_ls if tup[2] > 10]

    print(type_ls_json[:10])

    cn_types = list(cn_types)
    cn_types.sort()

    # this part exist only assuming transformation of simplified chinese is completed
    chinese_type_all_path = join(data_dir, 'types', 'chinese_types', 'wikidata_chinese_types_all_varients.txt')
    chinese_type_simplified_path = join(data_dir, 'types', 'chinese_types', 'wikidata_chinese_types_simplified.txt')
    with open(chinese_type_all_path, 'r') as r1, open(chinese_type_simplified_path, 'r') as r2:
        chinese_type_all = r1.readlines()
        chinese_type_simplified = r2.readlines()
        chinese_type_dict = {k.strip(): v.strip() for k, v in zip(chinese_type_all, chinese_type_simplified)}
    new_wikidata_ontology = []
    for d in type_ls_more_than_10:
        dd = json.loads(d)
        new_d = defaultdict(list)
        new_s = set()
        for cn in dd['cn']:
            new_s.add(chinese_type_dict[cn])
        new_d[dd['en']] = list(new_s)
        new_wikidata_ontology.append(json.dumps(new_d, ensure_ascii=False) + '\n')
    # type_ls_more_than_10 = new_wikidata_ontology

    with open(processed_300k_types_path, 'w') as w, \
            open(processed_10_up_types_path, 'w') as w2, \
            open(chinese_type_path, 'w') as w3 :
        # writer1 = csv.writer(w)
        # writer1.writerows(type_ls_json)
        # writer2 = csv.writer(w2)
        # writer2.writerows(type_ls_more_than_10)
        w.writelines(type_ls_json)
        w2.writelines(new_wikidata_ontology)
        w3.writelines([x + '\n' for x in cn_types])
        # w3.write(','.join(cn_types))




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
        distant_types = [x + '\n' for x in distant_types]
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
            else:
                had_set.add(x.strip())
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

    with open(comb_types, 'r') as r1, \
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
    get_ontology(add_trad=True)
    # write_ontology()
