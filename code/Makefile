all: data/wikipedia-sample uberjar

URL="http://dumps.wikimedia.org/enwiki/20130403/enwiki-20130403-pages-articles10.xml-p000925001p001325000.bz2"
SAMPLE_RATE="0.1"

clean:
	rm -rf target
	rm -rf data/wikipedia-sample
	rm -rf data/output

uberjar: target/pem.jar

streaming: lib/hadoop-streaming.jar

target/pem.jar:
	lein uberjar

lib/hadoop-streaming.jar:
	mkdir -p lib
	cp $(HADOOP_HOME)/contrib/streaming/hadoop-streaming-$(HADOOP_VERSION).jar lib/hadoop-streaming.jar

data/wikipedia.xml.bz2:
	curl -L -o data/wikipedia.xml.bz2 ${URL}

data/wikipedia/extracted: data/wikipedia.xml.bz2
	rm -rf data/wikipedia/extracted
	bzcat data/wikipedia.xml.bz2 | python bin/WikiExtractor.py -r ${SAMPLE_RATE} -o data/wikipedia/extracted

data/wikipedia-sample: streaming data/wikipedia/extracted
	rm -rf data/wikipedia-sample
	hadoop jar lib/hadoop-streaming.jar \
		-input 'data/wikipedia/extracted/*' \
		-output data/wikipedia-sample \
		-inputformat org.apache.hadoop.mapred.KeyValueTextInputFormat \
		-outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat \
		-mapper cat
