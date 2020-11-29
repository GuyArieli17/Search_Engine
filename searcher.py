from parser_module import Parse
from ranker import Ranker
import utils


class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        posting = utils.load_obj("posting")
        relevant_docs = {}
        for term in query:
            try: # an example of checks that you have to do
                posting_doc = posting[term] # list of all doc containt term
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = 1
                    else:
                        relevant_docs[doc] += 1
            except:
                print('term {} not found in posting'.format(term))
        return relevant_docs
 
 
        # q : donald trump had corana last week 
        # q-parse:  donald trump corana last week 
        # q-terms: [donald, trump,donald trump , corona, last,week]
        # covid week impact donald trump  = > [covid,week, impact,donald,trump,donald trump]
        # doc has all word in englist lung
      #
                # tf =  number of term in doc/number of word in doc 
                # idf = log( N(number of documents)/ number of doc with term)
                # w = tf * idf 
                #
                #
                #