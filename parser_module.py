from nltk.corpus import stopwords
from document import Document
import time

from stemmer import Stemmer


class Parse:
    def __init__(self,stemming):
        self.stemming=stemming
        self.toStem=Stemmer()
        self.terms_dic_to_document={}
      #  self.postingDict = {}
        self.lower_set = set()
        self.upper_set = set()
        self.numberList={"thousand":'K',"million":'M',"billion":'B',"percentage":'%',"percent":'%'}
        self.stop_words = stopwords.words('english')
        self.dict_stop_words={
            'a':[],'b':[],'c':[],'d':[],'e':[],'f':[],'g':[],'h':[],'i':[],'j':[],'k':[],'l':[],'m':[],'n':[],
            'o':[],'p':[],'q':[],'r':[],'s':[],'t':[],'u':[],'v':[],'w':[],'x':[],'y':[],'z':[]
        }
        for w in self.stop_words:
            self.dict_stop_words[w[0]].append(w)
        self.operators ={'*', '+', '-', '/', '<', '>', '&', '=', '|', '~','"'}
        self.parentheses ={'(', ')', '[', ']', '{', '}'}
        self.separators = {',', ';',':',' '}
        self.wird_symbols = {'!', '#', '$', '%', '&', '(', ')', ',', '*', '+', '-', '.', '/', ':', ';', '<', '=', '>', '?',
                        '@', '[', "'\'", ']', '^', '`', '{', '|', '}', '~', '}'}

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        indices=doc_as_list[4]
        retweet_text=doc_as_list[5]
        retweet_urls=doc_as_list[6]
        retweet_indices=doc_as_list[7]
        quoted_text=doc_as_list[8]
        quote_urls=doc_as_list[9]
        quoted_indices=doc_as_list[10]
        retweet_quoted_text=doc_as_list[11]
        retweet_quoted_urls=doc_as_list[12]
        retweet_quoted_indice=doc_as_list[13]
        term_dict = {}

        self.tokenSplit(full_text,term_dict)
        self.convertURL(url,term_dict)
        self.convertURL(retweet_urls,term_dict)
        self.convertURL(quote_urls,term_dict)
        self.tokenSplit(retweet_text,term_dict)
        self.tokenSplit(quoted_text,term_dict)

        """
        doc_length = len(tokenized_text)  # after text operations.
        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1
        """
        document = Document(tweet_id, tweet_date, full_text, url,indices,retweet_text,retweet_urls,retweet_indices,
                            quoted_text,quote_urls,quoted_indices,retweet_quoted_text,retweet_quoted_urls,retweet_quoted_indice,term_dict)
        return document

    def tokenSplit(self,text,term_dict):
        """
            This function tokenize, remove stop words and apply lower case for every word within the text
            :param text:
            :return:
        """
        if text==None:
            return []
        lst=[]
        word=''
        for i in text:
            if i not in self.operators and i not in self.parentheses and i not in self.separators and i!='\n':
                word+=i
            if (i==' ' or i == '/' or i==":" or i=='"' or i=='\n') and len(word)>=1:
                if self.stemming==True and word[0].lower() in self.dict_stop_words.keys():
                    if word.lower() not in self.dict_stop_words[word[0].lower()]:
                        word=self.toStem.stem_term(word)
                lst=self.addToken(lst,word,term_dict)
                word=''
        if len(word)>=1:
            if self.stemming == True and word:
                word = self.toStem.stem_term(word)
            lst=self.addToken(lst,word,term_dict)
        return lst

    def addToken(self,lst,word,term_dict):
        if word[-1]=='.' or word[-1]=='?' or word[-1]=='!':
            word=word[:-1]
            if len(word)==0:
                return lst
        if word[-1] != '…' and word[0].lower() in self.dict_stop_words.keys():
            if word.lower() not in self.dict_stop_words[word[0].lower()]:
                if word.lower() in self.numberList.keys() and len(lst)>=1:
                    if lst[-1].isnumeric():
                        #num=lst[-1]
                        lst[-1]+=self.numberList[word.lower()]
                        #if lst[-1] in self.terms_dic_to_document.keys():
                        #    self.terms_dic_to_document[lst[-1]] += self.terms_dic_to_document.pop(num)
                        #else:
                        #    self.terms_dic_to_document[lst[-1]]=self.terms_dic_to_document.pop(num)
                        #self.add_term_to_dict(lst[-1],term_dict)
                        self.add_term_numbers_to_dict(lst[-1],term_dict)
                        #term_dict[lst[-1]]=term_dict.pop(num)
                        #if lst[-1][-1]=="%":
                        #    self.lower_set.add(lst[-1])
                        #else:
                        #    self.upper_set.add(lst[-1])
                        #self.lower_set.remove(num)
                lst.append(word.lower())
                self.add_term_to_dict(word,term_dict)
        elif ((word.isascii() and word not in self.wird_symbols) or word.isnumeric() or word[0] == '#') and word[-1] != '…' and word[0] != "'":
            word = word.replace(",", "")
            if word.lower()=="million":
                print("x")
            if word[0].isdigit():
                if word.isnumeric():
                    try:
                        newW = self.convertNumber(int(word))
                    except:
                        try:
                            newW = self.convertNumber(float(word))
                        except:
                            newW=word
                    if newW != word:
                        lst.append(newW.lower())
                        self.add_term_to_dict(word,term_dict)
                elif len(word)>7:
                    for i in self.numberList.keys():
                        if i in word:
                            word=word.replace(i,self.numberList[i])
            if word[0] == "#":
                lst += self.find_sub_text_indexes(word,term_dict)
            lst.append(word.lower())
            self.add_term_to_dict(word,term_dict)
        return lst

    def convertURL(self,URL,term_dict):
        if URL==None:
            return []
        lst = []
        word = ''
        for i in URL:
            if i not in self.operators and i not in self.parentheses and i not in self.separators:
                word += i
            if (i == '/' or i==":" or i=='"') and len(word)>=1:
                #lst = self.addToken(lst, word)
                lst.append(word)
                self.add_term_to_dict(word,term_dict)
                word = ''
        #lst = self.addToken(lst, word)
        if len(word)>=1:
            lst.append(word)
            self.add_term_to_dict(word,term_dict)
        return lst

    def convertNumber(self,num):
        if num==None:
            return ""
        if num>=1000 and num<1000000:
            return str(num/1000)+'K'
        if num>=1000000 and num<1000000000:
            return str(num/1000000)+'M'
        if num>=1000000000:
            return str(num/1000000000)+'B'
        return str(num)

    def add_term_numbers_to_dict(self,term,term_dict):
        if term not in self.lower_set and term not in self.upper_set:
            self.upper_set.add(term.upper())
            term_dict[term.upper()] = 1
            self.terms_dic_to_document[term.upper()] = 1
        elif term in self.upper_set:
            if term not in term_dict.keys():
                term_dict[term.upper()] = 1
            else:
                term_dict[term.upper()] += 1
            self.terms_dic_to_document[term.upper()] += 1


    def add_term_to_dict(self,term,term_dict):
        if len(term)>0:
            if term.lower() not in self.lower_set and term.upper() not in self.upper_set:
                if (term[0].isupper() and (term[1:].islower() or term[1:].isupper()) and term.isalpha()) or term[-1].upper() in self.numberList.values():
                    self.upper_set.add(term.upper())
                    term_dict[term.upper()] = 1
                    self.terms_dic_to_document[term.upper()] = 1
                else:
                    self.lower_set.add(term)
                    term_dict[term.lower()] = 1
                    self.terms_dic_to_document[term.lower()] = 1
            elif term.lower() in self.lower_set:
                if term not in term_dict.keys():
                    term_dict[term.lower()] = 1
                else:
                    term_dict[term.lower()] += 1
                self.terms_dic_to_document[term.lower()] += 1
            elif term.upper() in self.upper_set:
                if term[0].isupper() or term[-1].upper() in self.numberList.values():
                    if term.upper() not in term_dict.keys():
                        term_dict[term.upper()] = 1
                    else:
                        term_dict[term.upper()] += 1
                    self.terms_dic_to_document[term.upper()] += 1
                else:
                    self.upper_set.remove(term.upper())
                    self.lower_set.add(term.lower())
                    self.terms_dic_to_document[term.lower()]=self.terms_dic_to_document[term.upper()]+1
                    del self.terms_dic_to_document[term.upper()]
                    if term.upper() in term_dict.keys():
                        term_dict[term.lower()]=term_dict[term.upper()]+1
                        del term_dict[term.upper()]
                    else:
                        term_dict[term.lower()]=1
    def find_sub_text_indexes(self, hashtag,term_dict):
        i = 0
        word = ''
        return_list = []
        for letter in hashtag[1:]:
            i += 1
            if (i == len(hashtag) - 1):
                i -= 1
            if letter.isupper():
                if (not word.isupper() and len(word) > 0) or hashtag[i + 1].islower():
                    if word != '#' and len(word)>0:
                        return_list.append(word.lower())
                        self.add_term_to_dict(word,term_dict)
                    word = ''
            elif word.isupper() and len(word) != 1:
                return_list.append(word.lower())
                self.add_term_to_dict(word,term_dict)
                word = ''
            elif letter == '_' and len(word)>0:
                return_list.append(word.lower())
                self.add_term_to_dict(word,term_dict)
                word = ''
                letter = ''
            word += letter
        if word != '':
            return_list.append(word.lower())
            self.add_term_to_dict(word,term_dict)
        return return_list
    """  
    def set_Upper_or_lower(self, word):
        # create dict for lower and upper list
        # run on th Word_list
        if not word[0].isupper():  # a lower word
            self.lower_set[word.lower()] = 0  # add word to lower dic
            if self.upper_dic.get(word.upper()) is not None:  # if appear in upper_dic
                indexes_list = self.upper_dic.get(word.upper())  # get list of indexes in list
                # run all over list and change to lower
                for upper_word_index in indexes_list:
                    word_list[upper_word_index] = word.lower()
                indexes_list = []  # no need to update more
        elif self.lower_set.get(word.lower()) is not None:  # Start with capital letter and in lower
            word_list[index] = word.lower()
        else:  # only seen as upper
            list_value = self.upper_dic.get(word.upper())  # if list already exist
            # update list if none
            if list_value is None:
                list_value = []
            list_value.append(index)  # add to list upper word
            self.upper_dic[word.upper()] = list_value  # add the list to dic
            word_list[index] = word.upper()  # change to upper word
        return word_list
    """

if __name__ == '__main__':
    p=Parse()
    x=p.tokenSplit('RT @_sheeba_j: @Dennylarashati1 Thank God RT of quarantine god don\'t have to wake up early Morning.. 😭#ดุลบาสเวิร์คช็อป#ดุลบาส https://…',{})
    print(p.terms_dic_to_document)


"""
    def add_term_to_dict(self,term,term_dict):
        if term not in term_dict.keys():
            term_dict[term] = 1
        else:
            term_dict[term] += 1
        if term not in self.terms_dic_to_document.keys():
            self.terms_dic_to_document[term]=1
        else:
            self.terms_dic_to_document[term]+=1
"""
"""
    def tokenSplit(self,text):
        lst=[]
        word=''
        for i in text:
            if i not in self.operators and i not in self.parentheses and i not in self.separators:
                word+=i
            if i==' ':
                if word[-1]=='.':
                    word=word[:-1]
                if word[-1] != '…' and word[0].lower() in self.dict_stop_words.keys():
                    if word.lower() not in self.dict_stop_words[word[0].lower()]:
                        if word.lower() in self.numberList:
                            lst[-1]+=word[0].upper()
                        lst.append(word.lower())
                elif ((word.isascii() and word not in self.wird_symbols) or word.isnumeric() or word[0] == '#') and word[-1] != '…' and word[0] != "'":
                    word = word.replace(",", "")
                    if word[0].isdigit():
                        try:
                            newW = self.convertNumber(int(word),'')
                        except:
                            newW = self.convertNumber(float(word),'')
                        if newW != word:
                            lst.append(newW.lower())
                    if word[0] == "#":
                        lst += self.find_sub_text_indexes(word)
                    lst.append(word.lower())
                word=''
        if word[-1] != '…':
            lst.append(word)
        return lst

    def bind_tokens_by_caps(self,list_tokens):
        return_list = []
        prev_string = ""
        for token in list_tokens:
            if token[0].isupper():
                if prev_string == "":
                    prev_string = token
                else:
                    prev_string += ' ' + token
            else:
                if prev_string != '':
                    return_list.append(prev_string)
                return_list.append(token)
                prev_string = ''
        if prev_string != '':
            return_list.append(prev_string)
        return return_list
    
    def expend_not_shortcut(self,word):
        return_value = word
        index_find = word.find("'")
        if index_find == -1:
            return return_value
        index__find = word.find("'t")
        if index__find != -1:
            if index_find > 0 and word[index_find-1] == 'n':
                index_find = index_find - 1
            return_value = word[0:index_find] + ' not'
        return return_value
        
    def convert_text_line_to_tokens(self,line):
        return nltk.word_tokenize(line)

    def convert_text_to_lines(self,text):
        return nltk.sent_tokenize(text)

    """

"""
        def parse_sentence(self, text):
            if text==None:
                return []
            text_tokens = word_tokenize(text)
            text_tokens_without_stopwords = []

            i=0
            for w in text_tokens:
                if w[-1]!='…' and w[0].lower() in self.dict_stop_words.keys():
                    if w.lower() not in self.dict_stop_words[w[0].lower()] and w.lower() not in self.numberList.keys():
                        text_tokens_without_stopwords.append(w.lower())
                elif ((w.isascii() and w not in self.wird_symbols) or w.isnumeric() or w[0]=='#') and w[-1]!='…' and w[0]!="'":
                    w=w.replace(",", "")
                    if w.isdigit():
                        try:
                            if i==len(text_tokens)-1:
                                newW=self.convertNumber(int(w),"")
                            elif i!=len(text_tokens)-1:
                                newW=self.convertNumber(int(w),text_tokens[i+1])
                            elif i==len(text_tokens)-1:
                                newW = self.convertNumber(float(w),"")
                            else:
                                newW= self.convertNumber(float(w),text_tokens[i+1])
                        except:
                            newW=w
                        if newW!=w:
                            text_tokens_without_stopwords.append(newW.lower())
                    if w[0]=="#":
                        text_tokens_without_stopwords+=self.find_sub_text_indexes(w)
                    text_tokens_without_stopwords.append(w.lower())
                i+=1
            return text_tokens_without_stopwords
    """