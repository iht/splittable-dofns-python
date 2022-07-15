
# Instructions

# Install Kafka

If you want to test your code against an actual Kafka server, follow the 
next steps to install Kafka in a local minikube cluster.


* Install minikube: https://minikube.sigs.k8s.io/docs/start/
* Make sure that you have an alias 
  - `alias k=kubectl`
* Create a namespace for Kafka: 
  - `k create namespace kafka`
* Install Kafka operator
  - `k create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka`
* Install single ephemeral cluster:
  - `k apply -f manifests/kafka-cluster.yaml -n kafka`
* Find out the port where Kafka is listening, and take note of it:
  - `k get service my-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}' -n kafka`
* Find out the local IP where Kafka is lesting, and take note of it:
  - `k get node minikube -o=jsonpath='{range .status.addresses[*]}{.type}{"\t"}{.address}{"\n"}'`

[//]: # ( * Redirect the port for external access)

[//]: # ( - `k port-forward service/my-cluster-kafka-external-bootstrap 50000:9094 --address='0.0.0.0' -n kafka`)

For your Kafka clients configuration, the bootstrap server will be `IP:PORT`.