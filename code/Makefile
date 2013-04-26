all: data/wikipedia-sample uberjar

URL="http://dumps.wikimedia.org/enwiki/20130403/enwiki-20130403-pages-articles10.xml-p000925001p001325000.bz2"

clean:
	rm -p target
	rm -rf data/wikipedia
	rm -rf data/output

uberjar: target/pem.jar

target/pem.jar:
	lein uberjar

data/wikipedia.xml.bz2:
	curl -L -o data/wikipedia.xml.bz2 ${URL}

data/wikipedia/extracted: data/wikipedia.xml.bz2
	bzcat data/wikipedia.xml.bz2 | python bin/WikiExtractor.py -r 0.1 -o data/wikipedia/extracted

data/wikipedia-sample: data/wikipedia/extracted
	find data/wikipedia/extracted -type f -print0 | xargs -0 cat > data/wikipedia-sample
