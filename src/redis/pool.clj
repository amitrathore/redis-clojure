(ns redis.pool
  (:import [java.io Reader BufferedReader InputStreamReader StringReader]
           [java.net Socket]
           [org.apache.commons.pool.impl GenericObjectPool]
           [org.apache.commons.pool BasePoolableObjectFactory])
  (:use redis.utils))

(defstruct connection
  :host :port :password :db :timeout :socket :reader :writer)

(def *pool* (atom nil))

(def *connection* (struct-map connection
                    :host     "127.0.0.1"
                    :port     6379
                    :password nil
                    :db       0
                    :timeout  5000
                    :socket   nil
                    :reader   nil
                    :writer   nil))

(defn connect-to-server
  "Create a Socket connected to server"
  [server]
  (let [{:keys [host port timeout]} server
        socket (Socket. #^String host #^Integer port)]
    (doto socket
      (.setTcpNoDelay true)
      (.setKeepAlive true))))

(defn new-redis-connection [server-spec]
  (let [connection (merge *connection* server-spec)
        #^Socket socket (connect-to-server connection)
        input-stream (.getInputStream socket)
        output-stream (.getOutputStream socket)
        reader (BufferedReader. (InputStreamReader. input-stream))]
    (assoc connection 
      :socket socket
      :reader reader
      :created-at (System/currentTimeMillis)))) 

(defn socket* []
  (or (:socket *connection*)
      (throw (Exception. "Not connected to a Redis server"))))

(defn send-command
  "Send a command string to server"
  [#^String cmd]
  (let [out (.getOutputStream (#^Socket socket*))
        bytes (.getBytes cmd)]
    (.write out bytes)))

(defn connection-factory [server-spec]
  (proxy [BasePoolableObjectFactory] []
    (makeObject []
      (new-redis-connection server-spec))
    (validateObject [c]
      (= "PONG" (binding [*connection* c]
                  (redis/ping))))
    (destroyObject [c]
      (.close (:socket c)))))

(defrunonce init-pool [server-spec]
  (println "init-pool")
  (let [factory (connection-factory server-spec)
        p (doto (GenericObjectPool. factory)
               (.setMaxActive 20)
               (.setTimeBetweenEvictionRunsMillis 10000)
               (.setWhenExhaustedAction GenericObjectPool/WHEN_EXHAUSTED_BLOCK)
               (.setTestWhileIdle true))]
    (reset! *pool* p)))

(defn get-connection-from-pool [server-spec]
  (if-not @*pool* 
    (init-pool server-spec))
  (.borrowObject @*pool*))

(defn return-connection-to-pool [c]
  (.returnObject @*pool* c))

(defn clear-pool []
  (.clear @*pool*))

(defn with-server* [server-spec func]
  (binding [*connection* (get-connection-from-pool server-spec)]
    (let [ret (func)]
      (return-connection-to-pool *connection*)
      ret)))
