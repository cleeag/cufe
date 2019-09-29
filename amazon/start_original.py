from flask import Flask, render_template, request, redirect
import json
import pandas as pd
app = Flask(__name__)


@app.route("/page/postdata", methods=["POST"])
def get_post_data():
    page_idx = int(request.values.get('page-idx', None))
    for n, v in request.values.items():
        if n.startswith('tag'):
            fout.write('{}\t{}\t{}\n'.format(page_idx, n, v))
            fout.flush()
    return redirect('/page/{}'.format(page_idx + 1))


@app.route("/")
def home():
    f = open(label_output_file, encoding='utf-8')
    page_idx = 0
    for line in f:
        cur_page_idx = int(line.strip().split('\t')[0])
        if cur_page_idx > page_idx:
            page_idx = cur_page_idx
    f.close()
    return redirect('/page/{}'.format(page_idx + 1))


# @app.route("/")
# def home():
#     return render_template('acl19_type_turker_flask.html', S2='fdsfds')


@app.route("/page/<page_idx>")
def hello(page_idx):
    page_idx = int(page_idx)
    data_row = df.iloc[page_idx - 1]
    doc_text = data_row['disp_str']
    mention_ids = json.loads(data_row['mention_ids'])
    mention_idxs_str = str([i for i in range(len(mention_ids))])
    doc_text = doc_text.replace('"', '\\"')
    return render_template('twitterfetannotate.html', doc_text=doc_text,
                           mention_idxs=mention_idxs_str, page_idx=page_idx)


if __name__ == '__main__':
    label_output_file = 'label-output.txt'
    anno_data_file = 'tweets-1217-nodup-text-anno.csv'
    with open(anno_data_file, encoding='utf-8') as f:
        df = pd.read_csv(f)
    fout = open(label_output_file, 'a', encoding='utf-8')
    app.run(debug=True, host='0.0.0.0')
