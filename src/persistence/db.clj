(ns persistence.db
  (:require [datomic.api :as d]
            [hodur-engine.core :as hodur]
            [hodur-datomic-schema.core :as hodur-datomic]))

(def db-uri "datomic:dev://localhost:4334/transactions")

(def trx-meta-db (hodur/init-schema
                   '[^{:datomic/tag-recursive true}
                     Transaction
                     [^{:type String
                        :doc "Transaction ID"}
                     id

                     ^{:type String
                       :doc "Transaction Merchant"}
                     merchant

                     ^{:type String
                       :doc "Merchant CNPJ"}
                     cnpj

                     ^{:datomic/type :db.type/bigdec
                       :doc "Transaction Price"}
                     price]]))

(def schema (hodur-datomic/schema trx-meta-db))

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

