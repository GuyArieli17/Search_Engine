
import math
import utils


class Ranker:

    def __init__(self):
        #lalal
        pass

    @staticmethod
    def get_relvant_docs(parse_query, posting):
        """
               This function loads the posting list and count the amount of relevant documents per term.
               :param query: query
               :return: dictionary of relevant documents.
               """
        # { doc_id: [doc tuple, list of the same terms](list in list)}
        relevant_docs = {}
        meta_data_dict = dict()
        for index in range(len(parse_query)):
            # for each document we will have the word they have the
            term = parse_query[index]
            try:  # an example of checks that you have to do
                posting_doc = posting[term]  # list of all doc containt term
                meta_data_dict[index] = (term, len(posting_doc))  # {index:(term , number of doc with term)}
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = [doc_tuple, {index}]
                    else:
                        relevant_docs[doc][1].add(index)
            except:
                print('term {} not found in posting'.format(term))
        relevant_docs["META-DATA"] = meta_data_dict
        return relevant_docs

    @staticmethod
    def weight_of_term(term_frequence, number_of_dcoument_in_compos, number_of_document_with_term, number_of_term_in_document=0, avg_doc_length=0):
        _k = 1.2
        _b = 0.75
        idf_term = math.log2(number_of_dcoument_in_compos /
                             number_of_document_with_term)
        document_punishment = 1
        # document_punishment = 1 - self._b + self._b * \
        #     (number_of_term_in_document/avg_doc_length)
        divider = (term_frequence * (_k + 1)) / \
            (term_frequence + _k * document_punishment)
        return idf_term * divider

    @staticmethod
    def simple_rank_doc_top_n(relevant_doc, number_of_doc=100):
        # qurey_parse = relevant_doc["Query_info-"]
        # get number of document in relevent docs
        number_of_dcoument_in_compos = len(relevant_doc)
        # get the Meta Data from relvant_doc
        meta_data_dict = relevant_doc["META-DATA"]
        # run on all relevant_doc
        for document_id, info_list in relevant_doc.items():
            # start with score 0
            doc_score = 0
            # get all doc information
            doc_tuple = info_list[0]
            # get from doc_tuple the number of time term appear
            number_of_term_in_document = doc_tuple[0]
            # get how many time appear from the doc_tuple
            term_frequence = 0
            intersection_terms = info_list[1]
            # run on all similar terms
            for term_index in intersection_terms:
                number_of_document_with_term = meta_data_dict[term_index][1]
                doc_score += Ranker.weight_of_term(term_frequence, number_of_dcoument_in_compos,
                                                   number_of_document_with_term, number_of_term_in_document)
            info_list.insert(0, doc_score) # [score,doc_tuple, {index}]
        # sort by the score
        if number_of_doc == 'All':
            return sorted(relevant_doc.items(), key=lambda item: item[0], reverse=True)
        return (sorted(relevant_doc.items(), key=lambda item: item[0], reverse=True))[:number_of_doc]
    
    @staticmethod
    def create_c_of_doc(top_relevant_docs, parse_qurey):
        # load map reduce from file
        map_reduce = utils.load_obj("posting")
        # relavent doc : # {doc_id : [score,doc_tuple, {index}]}
        # c[term,term2] = sum[k](term1 in doc k * term2 in doc k)
        #  = > {}
        c_matrix = {} # {term: {'other term' : value}}
        set = {}
        for index in range(len(parse_qurey)):
            term = parse_qurey[index]
            # check if already exist
            if term not in c_matrix.keys():
                c_matrix[term] = {}
            # build {term: {}}
            if term not in c_matrix.keys():
                c_matrix[term] = {}
            # run on all dox in map reduce
            term_sim_dic = {} #{other_term: value - > sum until now}
            for doc_id in top_relevant_docs.keys():
                if index in top_relevant_docs[doc_id][2]:
                    document_dictionary = map_reduce.read_from(('Document',doc_id))
                    sum = 0
                    for doc_term, freq in document_dictionary.items():
                        #
                        # if term == doc_term:
                        #     continue
                        # create new key
                        if doc_term not in term_sim_dic.keys():
                            term_sim_dic[doc_term] = 0
                        term_sim_dic[doc_term] += freq * document_dictionary[term]
            c_matrix[term] = term_sim_dic
        return c_matrix

    @staticmethod
    def create_association_matrix(c_matrix):

        # c_matrix will be a dic of dic  {term: {'other term' : value}}
        association_matrix = {}
        # dict build as first serch of i and then cearch j (dict inside a dict)
        for term in c_matrix.keys():
            # get all dict of all values association with terms
            association_terms_dict = c_matrix[term]
            # create a dic of all associate terms
            column_dict = {}
            association_matrix[term] = column_dict
            # run on the values and keys
            for term_key, value in association_terms_dict.items():
                column_dict[term_key] = (
                    value) / (c_matrix[term][term] + c_matrix[term_key][term_key] - value)
        return association_matrix

    @staticmethod
    def expand_qurey(parse_qurey, association_matrix):
        # from wich associatio we accept
        MIN_REQUIREDMENT = 0.6
        # the word we will insert
        insert_dic_by_term = {}  # {index: [word1,word2]}
        # run on all terms in qurey
        for index in range(len(parse_qurey)):
            # the term from query
            term = parse_qurey[index]
            # create a list to expand
            term_associated_term = []
            # save this list
            insert_dic_by_term[index] = term_associated_term
            # take the top association word with term
            for column in association_matrix[term]:
                for inner_term, associated_value in column.items():
                    #  column.item = { term : associated value}
                    if associated_value >= MIN_REQUIREDMENT:
                        term_associated_term.append(inner_term)
                # may be add a sort so added word will be sorted
        # how much the indexies changed
        prev_added = 1
        # run and add all the new words
        for index in insert_dic_by_term.keys():
            list_values = insert_dic_by_term[index]
            for value in list_values:
                parse_qurey.insert(index + prev_added, value)
                prev_added += 1
        insert_dic_by_term.clear()

    @staticmethod
    def rank_relevant_doc(relevant_doc,parse_query,posting):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        # get the best n docs for qurey (simple)
        top_relvant_docs = Ranker.simple_rank_doc_top_n(relevant_doc)
        #create c basic matrix to work with
        c_matrix = Ranker.create_c_of_doc(top_relvant_docs,parse_query)
        association_matrix = Ranker.create_association_matrix(c_matrix)
        c_matrix.clear()
        Ranker.expand_qurey(parse_query,association_matrix)
        return Ranker.simple_rank_doc_top_n(Ranker.get_relvant_docs(parse_query,posting),'All')

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
