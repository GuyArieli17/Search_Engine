import search_engine
from MapReduce import MapReduce
if __name__ == '__main__':



    search_engine.main("C:\\Users\\ayman\\Downloads\\Data1", "", False, ['Dr. Anthony Fauci wrote in a 2005 paper published in Virology Journal that hydroxychloroquine was effective in treating SARS.'], 5)
    """
    path= 'MapReduceData/'
    map_reduce = MapReduce(path=path)
    doc_0 ='doc0fgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfgfghjjjjjjjjjjjj'
    doc_1 ='doc1'
    doc_2 ='doc2'
    doc_30 ='doc30'
    doc_80 ='do80'
    # t =(doc_0,20),(doc_2,2),(doc_0,1),(doc_30,3),(doc_80,2)
    # t *= 6

    list = [(doc_0,20),(doc_2,2),(doc_0,1),(doc_30,3),(doc_80,2)]
    map_reduce.write_in('Guy',list)
    file_name = map_reduce.meta_data['Guy'][0][0]
    prev_byte,num_of_byte = [map_reduce.meta_data['Guy'][0][1], map_reduce.meta_data['Guy'][0][2]]
    nre_b = map_reduce.read_line('MapReduceData/' + str(file_name),[[prev_byte,num_of_byte]])
    print(map_reduce.meta_data['Guy'])
    print(nre_b)
    """
