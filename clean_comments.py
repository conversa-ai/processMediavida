import os
import json
import hashlib
import re
import pathlib
from cleantext import clean
import argparse

from config import SPANISH_FLAGGED_WORDS, TEXT_LENGTH_PATTERN, ANONYMIZE_URL_MAIL

TEXT_LENGTH_PATTERN = re.compile(r"^(#\d+\s*)?$")

def clean_text(text):
    text = clean(text,
          fix_unicode=True,  # fix various unicode errors
          to_ascii=False,  # transliterate to closest ASCII representation
          lower=False,  # lowercase text
          no_line_breaks=False,  # fully strip line breaks as opposed to only normalizing them
          no_urls=True,  # replace all URLs with a special token
          no_emails=True,  # replace all email addresses with a special token
          no_phone_numbers=True,  # replace all phone numbers with a special token
          no_numbers=False,  # replace all numbers with a special token
          no_digits=False,  # replace all digits with a special token
          no_currency_symbols=False,  # replace all currency symbols with a special token
          no_punct=False,  # remove punctuations
          replace_with_punct="",  # instead of removing punctuations you may replace them
          replace_with_url="<URL>",
          replace_with_email="<EMAIL>",
          replace_with_phone_number="<PHONE>",
          replace_with_number="<NUMBER>",
          replace_with_digit="0",
          replace_with_currency_symbol="<CUR>",
          lang="es"  # set to 'de' for German special handling
          )
    text = re.sub(r"<URL>(\n<URL>)?", "<URL>", text)
    return text

def process_comment(comment):
    user_hash = hashlib.sha256(comment["user"].encode()).hexdigest()[:8]
    # comment["user"] = user_hash
    matches = re.findall(r'#\d+', comment['content'])
    comments_list = []
    if not matches:
        comment_dict = {
            'parent_comment': -1,
            'comment_id': int(comment['order']),
            'user': user_hash,
            'comment_text': comment['content']
        }
        comments_list.append(comment_dict)
    else:
        for parent_comment_id in matches:
            parent_comment = int(parent_comment_id.replace('#', ''))
            if parent_comment >= int(comment['order']):
                parent_comment = -1
            comment_dict = {
                'parent_comment': parent_comment,
                'comment_id': int(comment['order']),
                'user': user_hash,
                'comment_text': comment['content']
            }
            comments_list.append(comment_dict)
    return comments_list

def check_text_length(comment):
    processed_text = re.sub(ANONYMIZE_URL_MAIL, "", comment['comment_text'])

    # Check if the length of the processed text is more than 10
    if len(processed_text) > 10:
        return True
    return False

def check_flagged_words(comment):
    for word in SPANISH_FLAGGED_WORDS:
        if word in comment["comment_text"].lower():
            return True
    return False

def process_comment_file(corpus_folder, file):
    # Read file
    with open(os.path.join(corpus_folder, file), 'r') as f:
        data = json.load(f)
    # Process comments
    comments_list = []
    for comment in data["objects"]:
        comments_list.extend(process_comment(comment))
    return comments_list

def get_children(comments_list):
    children = {}
    for comment in comments_list:
        if comment['parent_comment'] not in children:
            children[comment['parent_comment']] = []
        children[comment['parent_comment']].append(comment['comment_id'])
    children[-1] = [comment['comment_id'] for comment in comments_list if comment['parent_comment'] == -1]
    return children


def find_paths(node, current_path, tree, all_paths):
    current_path.append(node)

    # If the node has no children (it's a leaf node)
    if node not in tree or not tree[node]:
        all_paths.append(current_path.copy())
        return

    # If the node has children, iterate over each child and recursively find paths
    for child in tree[node]:
        find_paths(child, current_path, tree, all_paths)
        current_path.pop()  # backtrack

def get_all_paths(tree, root):
    all_paths = []
    find_paths(root, [], tree, all_paths)
    return all_paths

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_comment_folder', type=str, required=True)
    parser.add_argument('--output_folder', type=str, required=True)
    parser.add_argument('--output_folder_verbose', type=str, required=False)
    args = parser.parse_args()
    files = os.listdir(args.input_comment_folder)
    output_folder = args.output_folder
    pathlib.Path(output_folder).mkdir(parents=True, exist_ok=True)
    processed_files = os.listdir(output_folder)
    if args.output_folder_verbose:
        output_verbose_folder = args.output_folder_verbose
        pathlib.Path(output_verbose_folder).mkdir(parents=True, exist_ok=True)
    for file in files:
        if file.endswith(".json"):
            if file in processed_files:
                print(f'File {file} already processed')
                continue
            print(file)
            comments_list = process_comment_file(args.input_comment_folder, file)
            if not comments_list:
                continue
            comments_to_filter = []
            for comment in comments_list:
                if not check_text_length(comment) or check_flagged_words(comment):
                    comments_to_filter.append(comment['comment_id'])
            children = get_children(comments_list)
            root = -1
            all_paths = get_all_paths(children, root)
            print(f'All paths found {len(all_paths)}')

            if len(all_paths) > 100000:
                print(f'File {file} has too many paths. Skipping...')
                continue

            result = []

            for i in all_paths:
                if all(i not in j or i == j for j in all_paths):
                    result.append(i)

            # Remove duplicates
            dialogues = [list(t) for t in set(tuple(i) for i in result)]
            dialogues = [d[1:] for d in dialogues if len(set(comments_to_filter).intersection(set(d))) == 0 and len(d) > 2]
            print(f'Filtered dialogues {len(dialogues)}')
            if dialogues:
                unique_comments = set([item for sublist in dialogues for item in sublist])
                unique_comments_list = sorted([c for c in comments_list if c['comment_id'] in unique_comments], key=lambda x: x['comment_id'])
                unique_comments_list = [{'id': comment['comment_id'], 'user': comment['user'],
                                         'text': clean_text(comment['comment_text'])} for comment in unique_comments_list]
                dialogues_to_json = {i:d for i,d in enumerate(dialogues)}
                dialogues_compact_to_json = {'comments': unique_comments_list, 'dialogues': dialogues_to_json}
                if args.output_folder_verbose:
                    dialogues_verbose_to_json = {}
                    for i in dialogues_to_json:
                        dialogue_verbose = []
                        for order in dialogues_to_json[i]:
                            comment = [c for c in comments_list if c['comment_id'] == order][0]
                            dialogue_verbose.append(clean_text(comment['comment_text']))
                        dialogues_verbose_to_json[i] = dialogue_verbose

                with open(os.path.join(output_folder, file), 'w') as f:
                    json.dump(dialogues_compact_to_json, f)
                if args.output_folder_verbose:
                    with open(os.path.join(output_verbose_folder, file), 'w') as f:
                        json.dump(dialogues_verbose_to_json, f)


if __name__ == "__main__":
    main()