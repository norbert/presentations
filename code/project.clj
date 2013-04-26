(defproject pem-20130426 "0.1.0-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :uberjar-name "pem.jar"
  :aot [pem.core]
  :main pem.core
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "1.10.1"]
                 [cascalog-more-taps "0.3.0"]]
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-core "1.0.4"]]}})
