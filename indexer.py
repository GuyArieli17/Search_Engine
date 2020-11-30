from MapReduce import MapReduce
from nltk.corpus import util
from configuration import ConfigClass
from parser_module import Parse
import psutil
import sys

class Indexer:

    def __init__(self, config, all_terms_dict):
        self.inverted_idx = all_terms_dict
        self.postingDict = {}
        self.fileName = 'InvertedIndex'
        self.config = config
        # {term: [ordered list where appear : (file_id , lineNumber)]}
        avg_ram = (psutil.virtual_memory().available // 7)
        self.avg_length = (avg_ram // sys.getsizeof((int(), str()))) // (8/10)
        self.map_reduce = MapReduce(self.avg_length)
        self.tmp_pos = dict()


    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        max_term_list = dict()
        document_dictionary = document.term_doc_dictionary
        number_of_terms = 0
        for term in document_dictionary.keys():
            number_of_terms += 1
            if term in max_term_list.keys():
                max_term_list[term] += 1
            else:
                max_term_list[term] = 1
            try:
                # if len(self.tmp_pos) >= self.avg_length:
                #     self.map_reduce.write_dict(self.tmp_pos)
                #     self.tmp_pos.clear()
                lower_term = term.lower
                if lower_term not in self.postingDict.keys():
                    self.tmp_pos[lower_term] = []
                self.tmp_pos[lower_term].append((document.tweet_id, document_dictionary[term]))
            except:
                print('problem with the following key {}'.format(term[0]))
        # max_term = max(max_term_list.keys())


if __name__ == '__main__':
    p = Parse(True)
    parsed_document = p.parse_doc(['1280914835979501568', 'Wed Jul 08 17:21:09 +0000 2020', '70% @loganxtalor: Y’all Towson took away my housing cause of COVID and I literally didn’t know where I was gonna go. I was in such a bind. I…', '{}', '[]',
                                   'Y’all Towson took away my housing cause of COVID and I literally didn’t know where I was gonna go. I was in such a… https://t.co/i8IdrIKp2B', '{"https://t.co/i8IdrIKp2B":"https://twitter.com/i/web/status/1280659984628490246"}', '[[116,139]]', None, None, None, None, None, None])
    i = Indexer()
    i.add_new_doc(parsed_document)


"""       
# Update posting
                # run on term.lower of current doc
                if term.lower() in self.inverted_idx.keys():
                    # if appear in postingDic as upper but lower change to lower
                    if term.upper() in self.postingDict.keys():
                        self.postingDict[term.lower()] = self.postingDict[term.upper()]
                        del self.postingDict[term.upper()]
                    # if a new one init as a new list
                    if term.lower() not in self.postingDict.keys():
                        self.postingDict[term.lower()] = []
                        # unit.write(term.lower()) = []
                    self.postingDict[term.lower()].append((document.tweet_id, document_dictionary[term]))
                    # {term : }
                #if seen as upper
                elif term.upper() in self.inverted_idx.keys():
                    if term.upper() not in self.postingDict.keys():
                        self.postingDict[term.upper()] = []
                    self.postingDict[term.upper()].append((document.tweet_id, document_dictionary[term]))
                
                """
