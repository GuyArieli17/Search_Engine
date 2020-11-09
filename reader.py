# import os
# import pandas as pd
# import pyarrow.parquet as pq
#
# class ReadFile:
#     def __init__(self, corpus_path):
#         self.corpus_path = corpus_path
#
#     def read_file(self, file_name):
#         """
#         This function is reading a parquet file contains several tweets
#         The file location is given as a string as an input to this function.
#         :param file_name: string - indicates the path to the file we wish to read.
#         :return: a dataframe contains tweets.
#         """
#         full_path = os.path.join(self.corpus_path, file_name)
#         df = pd.read_parquet(full_path, engine="pyarrow")
#         return df.values.tolist()



import os
import pandas as pd
import pyarrow.parquet as pq

class ReadFile:
    def __init__(self, path):
        self.Path = path
        self.listFiles = []
        self.dict = {}
    def openFolder(self):
        # file=open(self.path, 'r')
        self.listFolders=os.listdir(self.Path)
        for s in self.listFolders:
            if s != '.DS_Store':
                self.listFiles.append(self.Path+'\\'+s)
    def openFiles(self):
        for f in self.listFiles:
            a = pq.read_table(source=f).to_pandas()


if __name__ == '__main__':
    r = ReadFile("C:\Code\Python\Data")
    r.openFolder()
    r.openFiles()
    print('Done')
