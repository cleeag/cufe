from flask import Flask, render_template, request, redirect, session
import json
import pandas as pd
from os.path import join
import random
# random.seed(42)

app = Flask(__name__)
home_dir = '/home/cleeag/cufe'

class DataStore():
    start = random.choice([x for x in range(1000)])
    end = start + 5

batch_no = DataStore()

@app.route("/postdata", methods=["POST"])
def get_post_data():
    print('getting data')
    cur_batch = batch[batch_no.start:batch_no.end]
    bars = ["GenType", "FineTypeA", "FineTypeB", "ImplicitTypeA", 'ImplicitTypeB']
    form_data = []
    for i in range(5):
        mention_res = []
        for bar in bars:
            type_res = request.values.get(bar + str(i), '')
            mention_res.append(type_res)
        mention_dict = {k: v for k, v in zip(['mention_id', 'mention'] + bars,
                                             [str(cur_batch[i]['mention_id']), cur_batch[i]['mention']] + mention_res)}
        error = request.form.get('NA' + str(i))
        if not error:
            # fout.write('{}: {}\t{}\t{}\t{}\t{}\n'.format(
            #     batch[i]['mention_id'], mention_res[0], mention_res[1], mention_res[2],
            #     mention_res[3], mention_res[4]
            # ))
            fout.write(json.dumps(mention_dict, ensure_ascii=False) + '\n')
        else:
            fout.write('{}: error\n'.format(cur_batch[i]['mention_id']))
        fout.flush()
        form_data.append(mention_res)
    print(form_data)
    # session['batch_start'] += 5
    # session['batch_end'] += 5
    batch_no.start += 5
    batch_no.end += 5
    return redirect("/")



@app.route("/")
def home():
    batch_mention = []
    for mention_dict in batch[batch_no.start:batch_no.end]:
        left = mention_dict['sentence'][:mention_dict['span'][0]]
        mention = mention_dict['mention']
        right = mention_dict['sentence'][mention_dict['span'][1]:]
        d = {'left': left, 'mention': mention, 'right':right}
        batch_mention.append(d)
    print(batch_no.start, batch_no.end)

    # aa = "<mark>He</mark>said he would take more time before resubmitting his team for approval."
    # return render_template('acl19_type_turker_flask.html', post=mention_dict, S1=mention_dict['sentence'])
    # return render_template('acl19_type_turker_org.html', post=batch_mention,
    #                        ontology_gen=json.dumps(ontology_gen), ontology_all=json.dumps(ontology_all))
    return render_template('acl19_type_turker_flask.html', post=batch_mention, test=True,
                           ontology_gen=json.dumps(ontology_gen), ontology_all=json.dumps(ontology_all))



if __name__ == '__main__':
    label_output_file = 'label-output.txt'
    # anno_data_file = 'tweets-1217-nodup-text-anno.csv'
    # cufe_mention_path = join(home_dir, 'data', 'cufe_mentions.txt')
    cufe_mention_path = 'cufe_mentions.txt'
    ontology_gen_path = join('..', 'types', 'general_types.txt')
    # ontology_all_path = join('..', 'types', 'types_en2cn.txt')
    ontology_all_path = join('..', 'types', 'crowd_types.txt')
    # from ontology import ontology
    with open(cufe_mention_path, 'r') as r:
        data = r.readlines()

    with open(ontology_gen_path, 'r') as r1, open(ontology_all_path, 'r') as r2:
        ontology_gen = r1.readlines()
        ontology_all = r2.readlines()
        # ontology_gen = [a.strip() for x in ontology_gen for a in [x.split('\t')[0], x]]
        # ontology_all = [a.strip() for x in ontology_all[9:] for a in [x.split('\t')[0], '\t'.join([x.split('\t')[1], x.split('\t')[0]])]]
        ontology_gen = [x.replace('\t', ' ').strip() for x in ontology_gen]
        ontology_all = [x.replace('\t', ' ').strip() for x in ontology_all[9:]]

        # ontology_all = [a.strip() for x in ontology_all[9:] for a in x.split('\t')]
    fout = open(label_output_file, 'a', encoding='utf-8')
    # session['batch_start'] = 0
    # session['batch_end'] = 5
    batch = [json.loads(x) for x in data]
    print('loading new')
    app.run(debug=True)
