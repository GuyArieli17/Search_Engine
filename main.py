import nltk
import re

import search_engine


def bind_tokens_by_caps(list_tokens):
    return_list = []
    prev_string = ""
    for token in list_tokens:
        if token[0].isupper():
            if prev_string == "":
                prev_string = token
            else:
                prev_string += ' ' + token
        else:
            if prev_string != '':
                return_list.append(prev_string)
            return_list.append(token)
            prev_string = ''
    if prev_string != '':
        return_list.append(prev_string)
    return return_list


def expend_not_shortcut(word):
    return_value = word
    index_find = word.find("'")
    if index_find == -1:
        return return_value
    index_find = word.find("'t")
    if index_find != -1:
        if index_find > 0 and word[index_find-1] == 'n':
            index_find = index_find - 1
        return_value = word[0:index_find] + ' not'
    return return_value


def convert_text_line_to_tokens(line):
    return nltk.word_tokenize(line)


def convert_text_to_lines(text):
    return nltk.sent_tokenize(text)


if __name__ == '__main__':
    """regex_example = "Avengers: Infinity War was a 2018 American superhero film based on the Marvel Comics superhero team the Avengers. It is the 19th film in the Marvel Cinematic Universe (MCU). The running time of the movie was 149 minutes and the box office collection was around 2 billion dollars. (Source: Wikipedia)"
    list = convert_text_to_lines(regex_example)
    for line in list:
        print(bind_tokens_by_caps(convert_text_line_to_tokens(line)))
    print('---------------------')"""
    search_engine.main()
    # print(bind_tokens_by_caps(['Avangers', 'Guy']))
    # bt = nltk.tag.
    # print(bt)

    # print(porter.stem("cats"))

    # print(re.findall('\d+', regex_example))  # line1
    # vowels = re.findall('[aeiou]', regex_example)  # line2
    # print(len(vowels))  # line 3
    # print(len(re.findall('Avengers', regex_example)))  # line 4
    # capitalwords = "[A-Z]\w+"  # line 5
    # print(re.findall(capitalwords, regex_example))  # line 6