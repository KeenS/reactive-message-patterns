(ns reactive-message-patterns.core
  (:use clojure.core.async))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

;;; Pipes and filters

(defn filter-actor
  ([f]
   (let [in (chan)
         out (chan)]
     (do
       (filter-actor f in out)
       [in out])))
  ([f in out]
   (go-loop []
     (if-let [v (<! in)]
       (if-let [ret (f v)]
         (if (>! out ret)
           (recur)))
       (close! out)))))

(let [[in out] (filter-actor (fn [x] (* x 2)))]
  (go
    ;; producer
    (doseq [v (range 1 10)]
      (>! in v))
    (close! in))
  (go-loop []
    ;; consumer
    (if-let [v (<! out)]
      (do
        (println v)
        (recur))
      (close! out))))


;;; Publish-Subscribe


(let [publisher (chan)
      publication (pub publisher :topic)
      subscriber1 (chan)
      subscriber2 (chan)]
  (sub publication :delete subscriber1)
  (sub publication :create subscriber1)

  (sub publication :update subscriber2)
  (sub publication :read subscriber2)

  ;; start subscribers before publish start
  (go-loop [] (when-let [v (<! subscriber1)] (printf "I'm One, got %s\n" (:type v)) (recur)))
  (go-loop [] (when-let [v (<! subscriber2)] (printf "I'm Two, got %s\n" (:type v)) (recur)))

  (go (onto-chan publisher [{:topic :update, :type "dog"}
                            {:topic :create, :type "cat"}
                            {:topic :read  , :type "fox"}]))
  )



;;; Invalid Message Channel
(defn invalid [data]
  {:data data
   :from (Thread/currentThread)})

(let [invalid-chan (chan)
      in (chan)
      tax 0.08]
  ;; invalid message processor
  (go-loop []
    (when-let [{data :data from :from} (<! invalid-chan)]
      (printf "invalid data %s from %s from\n" data from)
      (recur)))

  ;; main processing actor
  (go-loop []
    (when-let [v (<! in)]
      (if-let [price (:price v)]
        (printf "price: %f\n" (* price (+ 1 tax)))
        (>! invalid-chan (invalid v)))
      (recur)))

  (go (onto-chan in [{:goods "はじめてのClojure"
                      :price 1900}
                     {:goods "プログラミングClojure"
                      :price 3400}
                     {:goods "へび"
                      :description "にょろにょろ"}])))

;;; Document Message

(let [db [{:id 1 :name "Aho"} {:id 2 :name "Ulman"} {:id 3 :name "Sethi"}]
      in (chan)]
  (go-loop []
    (when-let [[id return] (<! in)]
      (>! return (some #(when (= id (:id %) ) %) db))
      (recur)))

  (let [chan-cache (chan)]
    (go-loop [id 1]
      (>! in [id chan-cache])
      (when-let [res (<! chan-cache)]
        (println res)
        (recur (+ id 1))))))
