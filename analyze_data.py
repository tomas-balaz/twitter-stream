import re
import numpy as np
from collections import defaultdict
from operator import itemgetter


def load_tweets_from_file(csv_file_name):
    with open(csv_file_name, "r", encoding='utf-8') as csv_file:
        tweet_dict = {}
        while line := csv_file.readline().rstrip():
            fields = line.split('\t')
            tweet_id = fields[1]
            tweet_dict[tweet_id] = line
        return tweet_dict


def get_tokens_and_count_from_text(tweet_text):
    return np.unique(re.sub(r'http\S+|[^\w\s]', ' ', tweet_text.lower()).split(), return_counts=True)


def add_tokens_to_index(index, tweet_text, tweet_id):
    tokens, token_count = get_tokens_and_count_from_text(tweet_text)
    for t, t_c in zip(tokens, token_count):
        index[t].append((tweet_id, t_c))


def sort_indexed_documents(index):
    for k in index.keys():
        index[k] = sorted(index[k], key=itemgetter(1), reverse=True)


def create_index(tweets_dict):
    index = defaultdict(list)
    for tweet_id, tweet in tweets_dict.items():
        fields = tweet.split('\t')
        if len(fields) < 10:
            print(f'START {tweet} END')
            exit()
        tweet_text = fields[9]
        add_tokens_to_index(index, tweet_text, tweet_id)
        pass
    sort_indexed_documents(index)
    return index


def search_token(index, token):
    # if token not in index.keys():
    #     print(f'Token \'{token}\' does not appear in any tweets')
    # for tweet_id in [t[0] for t in index[token]]:
    #     print(f'{tweets[tweet_id]}\n')
    return [t[0] for t in index[token]] if token in index.keys() else None


def get_tweet_ids_from_search(tokens, index, operation=None):
    tokens_incidence = [search_token(index, t) for t in tokens]
    if operation == "OR":
        tokens_incidence = filter(lambda x: x is not None, tokens_incidence)
        return set.union(*map(set, tokens_incidence))
    if None in tokens_incidence:
        return None
    if not operation:
        return tokens_incidence[0]
    if operation == "AND":
        return set.intersection(*map(set, tokens_incidence))


def parse_user_input(user_input):
    user_input = user_input.split()
    if len(user_input) == 1:
        return user_input, None
    elif len(user_input) > 1 and user_input[0] == 'AND':
        return user_input[1:], "AND"
    elif len(user_input) > 1 and user_input[0] == 'OR':
        return user_input[1:], "OR"
    else:
        return None


def main():
    tweet_count = 250000
    csv_file_name = f'csv_data_{tweet_count}_tweets.csv'
    print('loading tweets...')
    tweets = load_tweets_from_file(csv_file_name)
    print('creating index...')
    index = create_index(tweets)

    while True:
        user_input = str(input("insert text to search in format:\n" +
                               "\t1. single word to search\n" +
                               "\t2. [AND / OR] [space-separated words]\n"))
        if parser_output := parse_user_input(user_input):
            words, operation = parser_output
            if tweet_ids := get_tweet_ids_from_search(words, index, operation):
                for tweet_id in tweet_ids:
                    print(f'{tweets[tweet_id]}\n')
            else:
                print("Searched words do not occur in tweets.")
        else:
            print("Invalid user input.")
        print("\n==========================================\n")


if __name__ == "__main__":
    main()
