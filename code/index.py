import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, TextField, StringField
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.queryparser.classic import QueryParser
import sys, os


# my indexing function
def my_index(input_file, output_file):
    # init VM
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])

    # indexing part
    # init store and writer - code from VINF slides
    store = FSDirectory.open(Paths.get(output_file))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    # disable print output
    sys.stdout = open(os.devnull, 'w')

    # load the file and parse line by line
    # add all 3 field into the index
    with open(input_file) as file:
        for line in file:
            parts = line.replace('\n', '').split('\t')
            if(len(parts) == 3):
                doc = Document()
                doc.add(Field("name", parts[0], TextField.TYPE_STORED))
                doc.add(Field("df", parts[1], StringField.TYPE_STORED))
                doc.add(Field("cf", parts[2], StringField.TYPE_STORED))
                writer.addDocument(doc)

    # enable print, commit writer
    sys.stdout = sys.__stdout__
    writer.commit()
    writer.close()


if(len(sys.argv) > 2):
    my_index(sys.argv[1], sys.argv[2])
else:
    print('Not enough arguments')
