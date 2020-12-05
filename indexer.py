from MapReduce import MapReduce
from parser_module import Parse
import psutil
import sys

class Indexer:

    def __init__(self, config, all_terms_dict):
        self.inverted_idx = all_terms_dict
        #self.postingDict = {}
        self.fileName = 'InvertedIndex'
        self.config = config
        # {term: [ordered list where appear : (file_id , lineNumber)]}
        self.thread_pool_size = 5
        avg_ram = (psutil.virtual_memory().available // 7)//self.thread_pool_size
        self.avg_length =(avg_ram // sys.getsizeof((int(), str()))) // (8/10)
        self.map_reduce = MapReduce(self.avg_length,self.thread_pool_size)
        self.tmp_pos = {}
        self.num_in_pos_tmp = 0
        self.Entitys = {}

    """def create_c_matrix_dict(self,document_dictionary):
        c_matrix_dict={}
        for term1,freq1 in document_dictionary.items():
            if term1 not in c_matrix_dict.keys():
                term_sum={}
                for term2, freq2 in document_dictionary.items():
                    if term1!=term2:
                        term_sum[term1]+=freq1*freq2
                c_matrix_dict[term1]=term_sum
        return c_matrix_dict
    """

    def addEntitysToPosting(self,term,tweet_id,quantity):
        if term.upper() not in self.Entitys.keys() and term.upper() not in self.tmp_pos.keys():
            self.Entitys[term.upper()]=(tweet_id,quantity)
        else:
            if term.upper() not in self.inverted_idx.keys():
                self.inverted_idx[term.upper()]=2
            else:
                self.inverted_idx[term.upper()]+=1
            if term.upper() not in self.tmp_pos.keys():
                self.tmp_pos[term.upper()] = []
                self.tmp_pos[term.upper()].append(self.Entitys[term.upper()])
                del self.Entitys[term.upper()]
            self.tmp_pos[term.upper()].append((tweet_id,quantity))



    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        document_dictionary = document.term_doc_dictionary #{term:freq,term:freq}
        for term in document_dictionary.keys():
            try:
                if term[0].isupper() and " " in term:
                    self.addEntitysToPosting(term,document.tweet_id,document_dictionary[term])
                    continue
                if self.num_in_pos_tmp >= self.avg_length:
                    self.map_reduce.write_dict(self.tmp_pos)
                    self.tmp_pos.clear()
                    self.num_in_pos_tmp = 0
                if term.lower() not in self.tmp_pos.keys():
                    self.tmp_pos[term.lower()] = []
                self.tmp_pos[term.lower()].append((document.tweet_id, document_dictionary[term]))
                #max_freq = max([document_dictionary.values()])
                self.num_in_pos_tmp += 1
            except:
                print('problem with the following key {}'.format(term[0]))
        max_freq = max([document_dictionary.values()])
        self.tmp_pos[('Document', document.tweet_id)] = document_dictionary

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
