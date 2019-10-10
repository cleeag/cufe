import re
from os.path import join
from tqdm import tqdm
import json
import random

random.seed(42)

home_dir = '/home/cleeag/cufe'
data_dir = '/data/cleeag/wiki19'

def split_file():
    wiki_file_path = join(data_dir, 'zhwiki-20191001-pages-articles-multistream.xml')
    wiki_file_10k_path = join(data_dir, 'zhwiki-20191001-pages-articles-multistream_10k.xml')
    input_file = open(wiki_file_path, 'r')
    output_10k_file = open(wiki_file_10k_path, 'w')
    line = input_file.readline()
    j = 0
    while line:
        output_10k_file.write(line)
        line = input_file.readline()
        j += 1
        if j > 1000000:
            break
    return

def is_bad_line(line):
    bad = False
    bad_words = ['User:', 'xml', 'ref', 'File:', 'Wikipedia:', 'https:', 'http:', 'Help:', '&lt']
    bad_start = ['*', ':']

    if any([bad_word in line for bad_word in bad_words]):
        bad = True
    if not bad:
        if line[0] in bad_start:
            bad = True
    if not bad:
        if re.search(r'\[\[.*?:.*?\]\]', line):
            bad = True
    return bad

def is_bad_title(title):
    if 'wiki' in title.lower():
        return True


def get_wiki19_mention():
    wiki_file_path = join(data_dir, 'raw', 'zhwiki-20191001-pages-articles-multistream.xml')
    processed_sent_path = join(data_dir, 'zhwiki-20191001-sents.txt')
    processed_sent_sample_path = join(data_dir, 'zhwiki-20191001-sents_sample.txt')
    random_doc_path = join(data_dir, 'random_doc.txt')
    input_file = open(wiki_file_path, 'r')
    output_file = open(processed_sent_path, 'w')
    output_sample_file = open(processed_sent_sample_path, 'w')
    random_doc = open(random_doc_path, 'w')

    line = input_file.readline()
    good_sents, sent_count, line_count, sample_count = 0, 0, 0, 0
    article = []
    rolling = False
    article_title = ''
    content_dict = {}
    while line:
        line_count += 1
        if line_count % 1000000 == 0 and sent_count > 0:
            print(line_count, sent_count, good_sents, 'i/j=', round(good_sents/sent_count, 3))

        # if line.startswith('<text xml'):
        # if '</text>' in  line:
            # output_file.writelines([s + '\n' for s in sentences])
            # output_file.write(json.dumps({'sents': sentences}) + '\n')
            # if sample_count < 1000 and random.uniform(0 ,1) > 0.5:
            #     output_sample_file.write(json.dumps({'sents': sentences}, ensure_ascii=False) + '\n')
            #     sample_count += 1
            # sentences = []
        if re.search('<title>(.*?)</title>', line) and rolling == False:
            article_title = re.search('<title>(.*?)</title>', line).group(1)

        if line.strip().startswith('<text xml'):
            # print(line)
            rolling = True
            article.append(re.sub('<text.*?>', '', line))
        elif '</text>' not in  line and rolling:
            # print(article[:3])
            article.append(line)
        if '</text>' in  line and rolling:
            if not is_bad_title(article_title):
                sentences = []
                for i, line in enumerate(article):
                    if i == 0 and sample_count < 1000:
                        random_doc.write('\n')
                        random_doc.write(str(sample_count))
                        random_doc.writelines(article[:10])

                        # sentences.append(line)
                        # continue
                    while re.search(r'[。！？?]', line):
                        sent_count += 1
                        match_obj = re.search(r'[。！？?]', line)
                        cur_sent = line[:match_obj.start() + 1]
                        # if re.search(r'\[\[.*\]\]', cur_sent):
                        # if re.search(r'\[\[.*\]\]', cur_sent) and not is_bad_line(cur_sent):
                        if True:
                            sentences.append(cur_sent)
                            good_sents += 1

                        line = line[match_obj.start() + 1:]

                content_dict = {'title': article_title, 'sents': sentences}

                if len(content_dict['sents']) > 0:
                    output_file.write(json.dumps(content_dict, ensure_ascii=False) + '\n')
                    if sample_count < 1000 and random.uniform(0, 1) > 0.5:
                        output_sample_file.write(json.dumps(content_dict, ensure_ascii=False) + '\n')
                        sample_count += 1
            rolling = False
            article = []
            article_title = ''

        line = input_file.readline()

    input_file.close()
    output_file.close()
    output_sample_file.close()
    random_doc.close()
if __name__ == '__main__':
    # split_file()
    get_wiki19_mention()