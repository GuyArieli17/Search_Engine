from configuration import ConfigClass
from parser_module import Parse


class Indexer:

    def __init__(self, config,all_terms_dict):
        self.inverted_idx = all_terms_dict
        self.postingDict = {}
        self.config = config

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        document_dictionary = document.term_doc_dictionary
        # Go over each term in the doc
        for term in document_dictionary.keys():
            # write pos in disc and import it and work with it 
            try:
                # Update posting
                if term.lower() in self.inverted_idx.keys():
                    if term.upper() in self.postingDict.keys():
                        self.postingDict[term.lower()] = self.postingDict[term.upper()]
                        del self.postingDict[term.upper()]
                    if term.lower() not in self.postingDict.keys():
                        self.postingDict[term.lower()] = []
                    self.postingDict[term.lower()].append((document.tweet_id, document_dictionary[term]))
                elif term.upper() in self.inverted_idx.keys():
                    if term.upper() not in self.postingDict.keys():
                        self.postingDict[term.upper()]=[]
                    self.postingDict[term.upper()].append((document.tweet_id, document_dictionary[term]))
                    # 
            except:
                print('problem with the following key {}'.format(term[0]))

if __name__ == '__main__':
    p=Parse(True)
    parsed_document=p.parse_doc(['1280914835979501568', 'Wed Jul 08 17:21:09 +0000 2020', '70% @loganxtalor: Y’all Towson took away my housing cause of COVID and I literally didn’t know where I was gonna go. I was in such a bind. I…', '{}', '[]', 'Y’all Towson took away my housing cause of COVID and I literally didn’t know where I was gonna go. I was in such a… https://t.co/i8IdrIKp2B', '{"https://t.co/i8IdrIKp2B":"https://twitter.com/i/web/status/1280659984628490246"}', '[[116,139]]', None, None, None, None, None, None])
    i=Indexer()
    i.add_new_doc(parsed_document)


"""
        for term in document_dictionary.keys():
            try:
                oldTerm = term
                #if len(term)!=1:
                #   term = self.diceStemmers(term)
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term] += 1
                self.postingDict[term].append((document.tweet_id, document_dictionary[oldTerm]))

"""