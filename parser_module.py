import re

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
        text_tokens_with_hashtag=[]
        i = 0
        while i != len(text_tokens):
            if text_tokens[i]=='#':

                listHashTags=re.findall('[A-Z][^A-Z]*',text_tokens[i+1])
                for w in listHashTags:
                    text_tokens_with_hashtag.append(w.lower())
                text_tokens_with_hashtag.append(text_tokens[i]+text_tokens[i+1].lower())
                i+=2
            else:
                text_tokens_with_hashtag.append(text_tokens[i])
                i+=1

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


