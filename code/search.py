import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
import sys


# my searcher for created lucene index
def my_searcher(input_file):
    # init VM
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    
    # init searcher and directory - code from slides
    directory = FSDirectory.open(Paths.get(input_file))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer()

    # search given queries
    while(True):
        q = input('Enter query: ')
        if(q == ''):
            break;
        
        # query parsing and searching - first 20 docs
        query = QueryParser("name", analyzer).parse(q)
        scoreDocs = searcher.search(query, 20).scoreDocs

        # sorting documents by document frequency
        docs = []
        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            docs.append((doc.get("name"), doc.get("df"), doc.get("cf")))
        docs.sort(key=lambda x: int(x[2]), reverse=True)

        # print result
        print("%s/20 matches." % len(docs))
        for doc in docs:
            print('link name:', doc[0])
            print('  document frequency:', doc[1])
            print('  collection frequency:', doc[2])


if(len(sys.argv) > 1):
    my_searcher(sys.argv[1])
else:
    print('Not enough arguments')