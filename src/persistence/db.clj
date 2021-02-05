(ns persistence.db
  (:require [datomic.api :as d]))

(def db-uri "datomic:dev://localhost:4334/transactions")

(def schema [{:db/ident :transaction/id
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/doc "Transaction ID"}
             {:db/ident :transaction/merchant
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/doc "Transaction Merchant"}
             {:db/ident :transaction/cnpj
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/doc "Transaction CNPJ"}
             {:db/ident :transaction/price
              :db/valueType :db.type/bigdec
              :db/cardinality :db.cardinality/one
              :db/doc "Transaction Price"}])

(defn new-transaction [id merchant cnpj price]
  {:transaction/id id
   :transaction/merchant merchant
   :transaction/cnpj cnpj
   :transaction/price price})

(defn open-connection []
  (println "Opening DB connection")
  (d/create-database db-uri)
  (d/connect db-uri))

(defn erase-db []
  (println "Erasing DB")
  (d/delete-database db-uri))

(defn create-schema [conn]
  (println "Creating DB schema")
  (d/transact conn schema))

(defn save [conn trx]
  (println "Saving Transaction DB")
  (d/transact conn [trx]))

(defn get-all-transactions [conn]
  (d/q '[:find (pull ?entity [*])
         :where [?entity :transaction/id]] (d/db conn)))

