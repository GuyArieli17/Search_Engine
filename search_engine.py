import time
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils


def run_engine(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass(corpus_path, output_path, stemming)
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(stemming)
    indexer = Indexer(config, p.terms_dic_to_document)

    #  documents_list = r.read_file(file_name='sample3.parquet')
    # Iterate over every document in the file
    for i in r.filesPath:
        documents_list = r.read_file(i)
        start_time = time.time()
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = p.parse_doc(document)
            # update the number of doc in system
            number_of_documents += 1
            # index the document data
            indexer.add_new_doc(parsed_document)
        print(time.time() - start_time)
    print('Finished parsing and indexing. Starting to export files')
    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.map_reduce, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    #lalala
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    posting = utils.load_obj("posting")
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list,posting)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs,query_as_list,posting)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    run_engine(corpus_path, output_path, stemming,
               queries, num_docs_to_retrieve)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    # inverted_index = load_index()
    # for doc_tuple in search_and_rank_query(query, inverted_index, k):
    #     print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
