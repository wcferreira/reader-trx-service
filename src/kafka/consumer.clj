(ns kafka.consumer
  (:require [jackdaw.client :as jc]
            [jackdaw.client.log :as jl]
            [jsonista.core :as json]
            [persistence.db :as db]))

(def consumer-config
  {"bootstrap.servers" "localhost:9092"
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   "group.id" "consumer.reader-trx-service"
   "auto.offset.reset" "latest"})

(def topic-consumer-trx {:topic-name "paygo-transaction"})

(def conn (db/open-connection))

(db/create-schema conn)

(defn byte->json [value]
  (-> value
      (json/read-value json/default-object-mapper)))


(defn save-transaction-db [{:strs [id merchant cnpj price]}]
  (let [trx (db/new-transaction id merchant cnpj (bigdec price))]
      (db/save conn trx)))

(defn run-consumer []
  (with-open [my-consumer (-> (jc/consumer consumer-config)
                              (jc/subscribe [topic-consumer-trx]))]

    (doseq [{:keys [value]} (jl/log my-consumer 1000)]
      (save-transaction-db (byte->json value)))))

(defn get-trx-db []
  (db/get-all-transactions conn))


