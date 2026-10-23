# Deploying Apache Kafka on Kubernetes
This setup provides a solid foundation for building real-time data pipelines, event-driven applications, and microservices architectures.

## 1. Create Namespace:
```sh
kubectl create namespace kafka
```

## 2.Kafka Configuration: 
store Kafka configuration in a ConfigMap. (kafka-config.yaml)

```sh
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-config
data:
  server.properties.template: |
    process.roles=broker,controller
    node.id=${BROKER_ID}

    controller.quorum.voters=0@kafka-0.kafka-headless.kafka.svc.cluster.local:9093,1@kafka-1.kafka-headless.kafka.svc.cluster.local:9093,2@kafka-2.kafka-headless.kafka.svc.cluster.local:9093
    controller.listener.names=CONTROLLER

    listeners=PLAINTEXT://:9092,CONTROLLER://:9093
    advertised.listeners=PLAINTEXT://${POD_NAME}.kafka-headless.kafka.svc.cluster.local:9092

    listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
    inter.broker.listener.name=PLAINTEXT

    log.dirs=/var/lib/kafka/data

    num.partitions=3
    default.replication.factor=3
    min.insync.replicas=2
```


#### process.roles: 
Kafka will acts as broker and also controller.
#### advertised.listeners: 
Kafka brokers tell clients how to reach them. For pods inside the cluster, use kafka-$(HOSTNAME).kafka:9092 (DNS name). From outside of cluster, use external IP:port to connect in Kafka cluster.
#### num.partitions: 
How many brokers will store a copy of this topic’s partitions. Internal consumer offsets replicated on all 3 brokers.
#### default.replication.factor: 
How many copies of the transaction log exist. Here Transactions log replicated on all 3 brokers.
#### min.insync.replicas: 
A transaction will only commit if at least 2 replicas are in sync.

#### Now create this config map to store Kafka configuration in ConfigMap.
```sh
kubectl apply -f kafka-config.yaml -n kafka
kubectl get configmap -n kafka
```

## 3. Deploy Kafka StatefulSet:  
define a StatefulSet with 3 Kafka brokers in kafka-statefulset.yaml

```sh
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka-headless
  replicas: 3
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:4.4.0
          imagePullPolicy: IfNotPresent
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CLUSTER_ID
              value: "4L6g3nShT-eMCtK-X86sw"

          command:
            - sh
            - -euc
            - |
              export BROKER_ID=${HOSTNAME##*-}
              export POD_NAME=${HOSTNAME}

              sed "s/\${BROKER_ID}/${BROKER_ID}/g; s/\${POD_NAME}/${POD_NAME}/g" \
              /opt/kafka/config/server.properties.template > /tmp/server.properties

              /opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c /tmp/server.properties || true

              exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties

          ports:
            - containerPort: 9092
              name: broker
            - containerPort: 9093
              name: controller

          volumeMounts:
            - name: kafka-data
              mountPath: /var/lib/kafka/data
            - name: kafka-config
              mountPath: /opt/kafka/config/server.properties.template
              subPath: server.properties.template

      volumes:
        - name: kafka-config
          configMap:
            name: kafka-config

  volumeClaimTemplates:
    - metadata:
        name: kafka-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

#### export BROKER_ID=${HOSTNAME##*-} 
sets a unique broker ID from the pod name.
#### export POD_NAME=${HOSTNAME} 
saves the pod hostname for client access.
#### sed … 
replaces placeholders in the config template with actual broker ID and pod name.
#### kafka-storage.sh format … || true 
initializes Kafka storage
#### exec kafka-server-start.sh … 
starts the Kafka broker process.

#### Now create this statefulset and verify the pods.
```sh
kubectl apply -f kafka-statefulset.yaml -n kafka 
kubectl get statefulset -n kafka 
kubectl get pods -n kafka 
```

## 4. Create Headless service: 
define a headless service for kafka statefulset to get a fixed dns name for kafka brokers in kafka-headless-service.yaml
```sh
apiVersion: v1
kind: Service
metadata:
  name: kafka-headless
  labels:
    app: kafka
spec:
  clusterIP: None
  ports:
    - name: broker
      port: 9092
    - name: controller
      port: 9093
  selector:
    app: kafka
```

```sh
kubectl apply -f kafka-headless-service.yaml -n kafka
```

## 5. Create NodePort service: 
Define a nodeport service for external access. (nodeport-service.yaml)
```sh
apiVersion: v1
kind: Service
metadata:
  name: kafka-0-external
spec:
  type: NodePort
  ports:
    - port: 9092
      targetPort: 9092
      nodePort: 30090
  selector:
    statefulset.kubernetes.io/pod-name: kafka-0
```
```sh
 kubectl  apply -f nodeport-service.yaml -n kafka
```

## 6. Test:
Run a temporary pod to test Kafka from inside cluster. From shell we have created a topic, listed the topics, publish a message and consume the message from Kafka
```sh

root@ubuntu24-kind:/home/kafka# kubectl run kafka-client --rm -it --image=apache/kafka:4.4.0 -- bash
All commands and output from this session will be recorded in container logs, including credentials and sensitive information passed through the command prompt.
If you don't see a command prompt, try pressing enter.
kafka-client:/$
kafka-client:/$
kafka-client:/$ /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0.kafka-headless.kafka.svc.cluster.local:9092 \
  --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 3


Created topic test-topic.
kafka-client:/$
kafka-client:/$
kafka-client:/$ /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0.kafka-headless.kafka.svc.cluster.local:9092 \
  --describe \
  --topic test-topic

Topic: test-topic       TopicId: IURkew9URd6FanfIaH25fQ PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: test-topic       Partition: 0    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1      Elr:    LastKnownElr:
        Topic: test-topic       Partition: 1    Leader: 0       Replicas: 0,1,2 Isr: 0,1,2      Elr:    LastKnownElr:
        Topic: test-topic       Partition: 2    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0      Elr:    LastKnownElr:
kafka-client:/$
kafka-client:/$


```

