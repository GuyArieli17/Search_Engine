from configuration import ConfigClass


class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
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
            try:
                oldTerm = term
                if len(term)!=1:
                    term = self.diceStemmers(term)
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term] += 1
                self.postingDict[term].append((document.tweet_id, document_dictionary[oldTerm]))

            except:
                print('problem with the following key {}'.format(term[0]))

    def diceStemmers(self,word):
        lst1 = self.breakingDownWord(word)
        for w in self.inverted_idx.keys():
            count=0
            lst2=self.breakingDownWord(w)
            UnionList = list(set(lst1) | set(lst2))
            for i in lst1:
                count += lst2.count(i)
            if count/len(UnionList)>=0.8:
                return w
        return word

    def breakingDownWord(self,word):
        n = 2
        y=[word[i:i + n] for i in range(0, len(word), 1)]
        return y[:len(y)-1]
