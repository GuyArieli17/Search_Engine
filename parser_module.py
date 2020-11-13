import re

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document

class Parse:
    def __init__(self):
        self.stop_words = stopwords.words('english')

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        i = 0
        text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        text_tokens_with_tags = []
        i=0
        while i!=len(text_tokens_without_stopwords):
            if text_tokens_without_stopwords[i]=='@':
                text_tokens_with_tags.append(text_tokens_without_stopwords[i]+text_tokens_without_stopwords[i+1])
                i+=2
            else:
                text_tokens_with_tags.append(text_tokens_without_stopwords[i])
                i+=1
        for i in text_tokens_without_stopwords:
            self.convertURL(i)
            self.convertNumber()
        return text_tokens_with_tags

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    def bind_tokens_by_caps(self,list_tokens):
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

    def expend_not_shortcut(self,word):
        return_value = word
        index_find = word.find("'")
        if index_find == -1:
            return return_value
        index__find = word.find("'t")
        if index__find != -1:
            if index_find > 0 and word[index_find-1] == 'n':
                index_find = index_find - 1
            return_value = word[0:index_find] + ' not'
        return return_value

    def convert_text_line_to_tokens(self,line):
        return nltk.word_tokenize(line)

    def convert_text_to_lines(self,text):
        return nltk.sent_tokenize(text)

    def find_sub_text_indexes(self,hashtag):
        word = ''
        return_list = []
        for letter in hashtag:
            if letter.isupper():
                if not word.isupper() and len(word) >0 :
                    return_list.append(word)
                    word = ''
            elif word.isupper() and len(word) != 1:
                return_list.append(word)
                word = ''
            word += letter
        if word != '':
            return_list.append(word)
        return return_list

    def set_Upper_or_lower(self,word_list):
        # create dict for lower and upper list
        lower_set = dict()
        upper_dic = dict()
        # run on th Word_list
        for index in range(len(word_list)):
            word = word_list[index]  # get the word in index
            if not word[0].isupper():  # a lower word
                lower_set[word.lower()] = 0  # add word to lower dic
                if upper_dic.get(word.upper()) is not None:  # if appear in upper_dic
                    indexes_list = upper_dic.get(word.upper())  # get list of indexes in list
                    # run all over list and change to lower
                    for upper_word_index in indexes_list:
                        word_list[upper_word_index] = word.lower()
                    indexes_list = []  # no need to update more
            elif lower_set.get(word.lower()) is not None:  # Start with capital letter and in lower
                word_list[index] = word.lower()
            else:  # only seen as upper
                list_value = upper_dic.get(word.upper())  # if list already exist
                # update list if none
                if list_value is None:
                    list_value = []
                list_value.append(index)  # add to list upper word
                upper_dic[word.upper()] = list_value  # add the list to dic
                word_list[index] = word.upper() # change to upper word
        return word_list

    def convertURL(self,URL):
        _treebank_word_tokenizer = nltk.NLTKWordTokenizer()
        lst=URL.split('/')
        y=[token for sent in lst for token in _treebank_word_tokenizer.tokenize(sent)]
        lstToken=[]
        for i in y:
            if i[0].isalpha() or i[0].isdigit():
                lstToken.append(i)
        return [w for w in lstToken if w not in self.stop_words]
        return lstToken

    def convertNumber(self,num,str):
        if num>=1000 and num<1000000:
            return num/1000+'K'
        if num>=1000000 and num<1000000000:
            return num/1000000+'M'
        if num>=1000000000:
            return num/1000000000+'B'
        if str.lower()=="thousand":
            return num+'K'
        if str.lower()=="million":
            return num+'M'
        if str.lower()=="billion":
            return num+'B'
        if str.lower()=="percent" or str.lower()=="percentage":
            return num+"%"

if __name__ == '__main__':
    p=Parse()
    print(p.stop_words)