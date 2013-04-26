(ns pem.core
  (:use [cascalog.api]
        [cascalog.more-taps])
  (:require [clojure.string :as s]
            [cascalog [ops :as c] [vars :as v]])
  (:import [org.apache.hadoop.io Text])
  (:gen-class))

(defmapcatop tokenize [line]
  (re-seq #"(?i)\b\w\w+\b" line))

(defn etl-docs-gen [docs stop]
  (<- [?doc-id ?word]
      (docs ?key ?value)
      (str ?key :> ?doc-id) (str ?value :> ?body)
      (tokenize ?body :> ?word-dirty)
      ((c/comp s/trim s/lower-case) ?word-dirty :> ?word)
      (stop ?word :> false)))

(defn word-count [src]
  (<- [?word ?count]
      (src _ ?word)
      (c/count ?count)))

(defn D [src]
  (let [src (select-fields src ["?doc-id"])]
    (<- [?n-docs]
        (src ?doc-id)
        (c/distinct-count ?doc-id :> ?n-docs))))

(defn DF [src]
  (<- [?df-word ?df-count]
      (src ?doc-id ?df-word)
      (c/distinct-count ?doc-id ?df-word :> ?df-count)))

(defn TF [src]
  (<- [?doc-id ?tf-word ?tf-count]
      (src ?doc-id ?tf-word)
      (c/count ?tf-count)))

(defn tf-idf-formula [tf-count df-count n-docs]
  (->> (+ 1.0 df-count)
    (div n-docs)
    (Math/log)
    (* tf-count)))

(defn TF-IDF [src]
  (let [n-doc (first (flatten (??- (D src))))]
    (<- [?doc-id ?tf-idf ?tf-word]
        ((TF src) ?doc-id ?tf-word ?tf-count)
        ((DF src) ?tf-word ?df-count)
        (tf-idf-formula ?tf-count ?df-count n-doc :> ?tf-idf))))

(defn -main [stop in out tfidf & args]
  (let [docs (hfs-wrtseqfile in Text Text :outfields ["key" "value"])
        stop (hfs-delimited stop)
        src (etl-docs-gen docs stop)]
    (?- (hfs-delimited tfidf)
        (TF-IDF src))
    (?- (hfs-delimited out)
        (word-count src))))
