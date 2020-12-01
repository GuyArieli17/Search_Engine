import io
import pickle
import zlib
import os
import concurrent.futures
from multiprocessing import Lock
from asyncio import Semaphore
import asyncio


class MapReduce:
    def __init__(self, MAX_LINE_IN_FILE,thread_pool_size,path):
        #idk
        self.meta_data = {} #{term: [(self.file_index, self.line_number, number_of_lines)]]
        self.line_number = 0
        self.file_index = 0
        self.thread_pool_size = thread_pool_size
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_size)
        self.MAX_IN_FILE = MAX_LINE_IN_FILE
        self.process_semaphore = Semaphore(thread_pool_size)
        self.update_lock = Lock()
        self.path= path

    def update_meta_data(self,term,number_of_lines):
        #  if seen first time create a empty one
        if term not in self.meta_data.keys():
            self.meta_data[term] = []
        self.meta_data[term].append((self.file_index, self.line_number, number_of_lines))
        self.line_number += number_of_lines

    def update_files(self):
        if self.line_number >= self.MAX_IN_FILE:
            self.line_number = 0
            self.file_index += 1

    def write_dict(self,dic):
        self.executor.submit(self.write_dict_func,dic) #[self,dic]

    def write_dict_func(self,dic):
        asyncio.run(self.write_dict_func_async(dic))

    async def write_dict_func_async(self,dic):
        await self.process_semaphore.acquire()
        try:
            # print('Process_' + str(self.process_counter) + ': start writing')
            # key = term
            # value = [(doc1,freq_doc1)]
            for key, value in dic.items():
                self.write_in(key, value)
            #  check if need to create a new file afterward
        finally:
            self.process_semaphore.release()
            return True

    def write_in(self,term, data_list):
        #  file name = (dic location) / the possible file to write in
        file_name = self.path + str(self.file_index)
        # only one process can update
        self.update_lock.acquire()
        # save as (file_index,line_number_start,length)
        self.update_meta_data(term,len(data_list))
        # update number of line after add of data_list
        self.update_files()
        self.update_lock.release()
        # add data_list to file_name
        self.append_line(data_list, file_name)

    def append_line(self, data_list, file_name):
        """
        Gets: file name and data to save
        Does: save the data as bytes and compress them
        """
        #add to the file
        with open(file_name + '.comp', "ab") as fd:
            if isinstance(data_list,list):
                for data_tuple in data_list:
                    bytes = io.BytesIO()
                    # convert data into bytes as Bytes
                    pickle.dump(data_tuple, bytes)
                    # compress the byte and insert into zbytes
                    zbytes = zlib.compress(bytes.getbuffer())
                    fd.write(zbytes)
            # how to add dic
            elif isinstance(data_list,dict):
                bytes = io.BytesIO()
                # convert data into bytes as Bytes
                pickle.dump(data_list, bytes)
                # compress the byte and insert into zbytes
                zbytes = zlib.compress(bytes.getbuffer())
                fd.write(zbytes)

    def read_from(self, term):
        """We want to return the list of doc with info about the term"""
        future = self.executor.submit(self.read_from_func,term)
        return future.result()

    def read_from_func(self, term):
        return asyncio.run(self.read_from_func_async(term))

    async def read_from_func_async(self,term):
        data_list = []
        # {(term: [])}
        await self.process_semaphore.acquire()
        try:
            term_meta_data = self.meta_data[term]
            # save dic so duplicate will be together
            dic_by_file_name = {} # {file_name: sets of lines in file}}
            # run and build file and all lines in this file
            for file_index, file_line in term_meta_data:
                if file_index not in dic_by_file_name:
                    dic_by_file_name[file_index] = {}
                dic_by_file_name[file_index].add(file_line)

            organize_dic = {} # {(file_index,line_number)}
            # read file by file
            for file_index, file_line_set in dic_by_file_name.items():
                file_name = self.path + str(file_index)
                dic_of_current_text_by_line = self.read_line(file_name, file_line_set)
                for line_index, line_object in dic_of_current_text_by_line.items():
                    organize_dic[(file_index, line_index)] = line_object
            # {term: [(self.file_index, self.line_number, number_of_lines)]]
            # build list as organize
            for i in range(len(term_meta_data)):
                current_file_index, current_line_number, _ = term_meta_data[i]
                data_list.append(organize_dic[(current_file_index, current_line_number)])
        finally:
            self.process_semaphore.release()
            return data_list

    def read_line(self, file_name,line_number_set):
        """
        Gets: set of line to read from fuke
        Does: return the original object of file
        """
        data = {} #{line_number: obj}
        if os.path.isfile((file_name + '.comp')):
            with open(file_name + '.comp', 'rb') as fd:
                for i, line in enumerate(fd):
                    if i in line_number_set:
                        bytes = zlib.decompress(line)
                        data.append(pickle.loads(bytes))
        return data