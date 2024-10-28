from typing import Iterable
from nltk.corpus import stopwords
import re
import spacy


nlp = spacy.load('ru_core_news_sm', disable=["tagger", "parser", "senter", "morphologizer",
                                             "attribute_ruler", "lemmatizer"])
stop_words = set(stopwords.words('russian'))
stop_words = stop_words.union({'хвс', 'гвс', 'п', 'з', '.', 'откл'})
stop_words = stop_words.difference({'от', 'до'})


# токенизировать текст, предварительно разделив смешанные между собой текст и числа
def tokenize_comment(text: str) -> list[str]:
    pattern = r"(\d+[А-я]|\d+[A-z]|\d+|[A-z]+|[А-я]+|[-.()])"
    str_int_split = re.findall(pattern, text)
    return str_int_split


# фильтровать ненужные токены. последовательности вида 'Д', '{число}' фильтровать, избегая
# фильтрования посл. вида 'д', '.', '{число}'. фильтровать числа в скобках, оставляя слова.
def filter_tokenized_comment(tokenized_text: Iterable[str]) -> list[str]:
    filtered_tokens = []
    auto_filter = False
    parentheses_check = False

    for word in tokenized_text:
        if word.lower() in stop_words:
            continue

        if parentheses_check:
            if word.isdigit():  # пропустить цифры в скобках
                continue

            if word == ')':  # проверить закрывается ли скобка
                parentheses_check = False
                continue

            # остальные токены сохранить

        elif word == '(':  # проверка скобок только в случае если флаг скобок ложен
            parentheses_check = True
            continue

        if word.lower() == 'д':  # активировать фильтр лишних чисел
            auto_filter = True
            continue

        # фильтр конструкций типа 'Д=100' с защитой к-й типа 'д. 10' от фильтрации
        if auto_filter and (word.isdigit() or word == '.'):
            auto_filter = False
            continue

        filtered_tokens.append(word)

    return filtered_tokens


# Named Entity Recognition
def find_streets(text: str) -> list[str]:
    doc = nlp(text)
    ents = [ent.text for ent in list(doc.ents)]

    for i in range(len(ents)):
        ents[i] = ' '.join(sorted([word for word in ents[i].split() if len(word) > 2],
                                  key=lambda x: (x[0].isupper(), len(x))))

    return ents


# сформировать запрос. проанализировать прямо и косвенно указанные адресы. предполагается, что в
# фильтрованном списке токенов включают цифры только токены, обозначающие номера домов.
def form_queries(street_names: Iterable[str], tokenized_text: Iterable[str]) -> str:

    # создание словаря с ключами из названий улиц и значениями из списка номеров домов на данн. улице
    search_addresses = dict()
    current_street = ''
    home_id_pattern = r"(\d+[А-я]|\d+[A-z]|\d+)"

    for token in tokenized_text:
        for street in street_names:
            if token.strip().lower() in street.strip().lower():
                current_street = street.split()[-1]
                continue

        if not re.search(home_id_pattern, token) or not current_street:
            continue

        # TODO: - process number ranges.
        #       - look for even and unven range indicators.

        if current_street not in search_addresses:
            search_addresses[current_street] = []
        search_addresses[current_street].append(token)

    # составление паттерна поиска из списка адресов
    per_street_ptrns = []

    for key in search_addresses.keys():
        one_ptrn = '(?=.*' + re.escape(key) + ')(?=.*('
        one_ptrn += '|'.join([re.escape(value) for value in search_addresses[key]]) + '))'
        per_street_ptrns.append(one_ptrn)

    pattern = '|'.join(per_street_ptrns)

    return pattern


# преобразовать комментарий в список запросов для поиска по базе адресов
def process_comment(comment: str):

    # токенизировать и отфильтровать комментарий

    word_tokens = tokenize_comment(comment)
    filtered_tokens = filter_tokenized_comment(word_tokens)
    filtered_text = ' '.join(filtered_tokens)

    # преобразовать комментарий в список поисковых запросов

    street_names = find_streets(filtered_text)
    search_pattern = form_queries(street_names, filtered_tokens)
    # print(search_pattern)

    return search_pattern
