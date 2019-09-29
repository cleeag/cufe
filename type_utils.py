import json
import os
from os.path import join
from tqdm import tqdm
import random
import re

data_dir = '/home/cleeag/cufe/data'

def get_ontology():
    great_noun_list_path = join(data_dir, 'types', 'ontology', 'the_great_noun_list.txt')
    ufet_ontology_path = join(data_dir, 'types',  'ontology', 'ufet_ontology.txt')
    # all_types_path = join(data_dir, 'types', 'all_types.txt')
    all_types_path = join(data_dir, 'types', 'crowd_types.txt')
    yago_types_path = join(data_dir, 'types', 'ontology', 'yago_types.txt')

    # filters
    country_list_path = join(data_dir, 'types', 'ontology', 'country_list.txt')
    yt_bad_words_path = join(data_dir, 'types', 'ontology', 'yt_bad_words.txt')

    ontology_output_path = join(data_dir, 'types', 'ontology', 'cufe_ontology.txt')
    with open(great_noun_list_path, 'r') as r1, open(ufet_ontology_path, 'r') as r2, \
            open(all_types_path, 'r') as r3, open(yago_types_path, 'r') as r4, \
            open(country_list_path, 'r') as r5, open(yt_bad_words_path, 'r') as r6:
        great_noun_list = r1.readlines()
        ufet_ontology = json.loads(r2.read())
        all_types = r3.readlines()
        yago_types = r4.readlines()
        country_set = set([x.strip().lower() for x in r5.readlines()])
        yt_bad_words = set([x.strip().lower() for x in r6.read().split(',')])

    great_noun_list = [' '.join(x.strip().split('-')) for x in great_noun_list]
    # all_types = [' '.join(x.strip().split('_')).lower() for x in all_types]
    all_types = [' '.join(x.strip().split('\t')[0].split('_')) for x in all_types]
    ufet_ontology = [x for x in ufet_ontology]
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


    print(len(great_noun_list), len(ufet_ontology), len(all_types), len(new_yago_types))
    print(great_noun_list[:10], ufet_ontology[:10], all_types[:10], new_yago_types[:10])
    great_noun_list = [x.lower()  for x in great_noun_list]
    ufet_ontology = [x.lower()  for x in ufet_ontology]
    all_types = [x.lower()  for x in all_types]
    new_yago_types = [x.lower()  for x in new_yago_types]
    gs = set(great_noun_list)
    # us = set(ufet_ontology)
    # alls = set(all_types)
    # gs.update(all_types)
    # print(len(gs))
    # us.update(all_types)
    # print(len(us))
    gs.update(ufet_ontology)
    gs.update(all_types)
    gs.update(new_yago_types)
    gs = gs - country_set
    filtered_gs = set()
    for word in gs:
        dirty = False
        for w in word.split(' '):
            for prof in yt_bad_words:
                if prof == w:
                    print(prof, word)

                    dirty = True
                    break
            if dirty:
                break
        if not dirty:
            filtered_gs.add(word)

    ontology = [x.lower() + '\n' for x in filtered_gs]
    ontology.sort()
    print(yt_bad_words)
    print(len(ontology))
    ontology_js_path = join(data_dir, 'types', 'js', 'cufe_ontology.js')
    with open(ontology_output_path, 'w') as w, open(ontology_js_path, 'w') as w2:
        w.writelines(ontology)
        ontology_str = '"' + '","'.join([x.strip() for x in ontology]) + '"'
        w2.write(f"""var ontology=[{ontology_str}];""")




def types_to_js():
    crowd_types = join(data_dir, 'types', 'crowd_types.txt')
    general_types = join(data_dir, 'types', 'general_types.txt')
    with open(crowd_types, 'r') as r1, open(general_types, 'r') as r2:
        crowd_types_ls = [x.strip().split('\t')[0] for x in r1.readlines()]
        general_types_ls = [x.strip().split('\t')[0] for x in r2.readlines()]
        # crowd_types_ls = [x.strip() for x in r1.readlines()]
        # general_types_ls = [x.strip() for x in r2.readlines()]

    crowd_types = join(data_dir, 'types', 'js', 'crowd_types.js')
    general_types = join(data_dir, 'types', 'js', 'general_types.js')
    with open(crowd_types, 'w') as w1, open(general_types, 'w') as w2:
        crowd_str = '"' + '","'.join(crowd_types_ls) + '"'
        w1.write(f"""var crowd_types=[{crowd_str}];""")
        general_str = '"' + '","'.join(general_types_ls) + '"'
        w2.write(f"""var general_types=[{general_str}];""")


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
        # for x in zip(org_types, cn_types):
        #     tmp = '\t'.join([y.strip() for y in x])
        #     print(tmp)



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
    get_ontology()