# Splittable DoFns in Python: A Hands-On Workshop

[![Python Versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/apache--beam-latest-orange)](https://beam.apache.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This repository contains the code samples and exercises used for the workshop **"Splittable DoFns in Python"** at **Beam Summit 2022**.

## 🎥 Resources

*   **Workshop Recording:** [Watch on YouTube](https://www.youtube.com/watch?v=VQdtaaWxN0Y)
*   **Presentation Slides:** [Download Slides (PDF)](docs/slides.pdf)
*   **Official Session Page:** [Beam Summit 2022 Session](https://2022.beamsummit.org/sessions/splittable-dofns-in-python/)

## 🌿 Repository Structure

This repository is organized into two main branches:

*   [`main`](https://github.com/iht/splittable-dofns-python/tree/main): Template branch containing the exercises. **Start here** and write your code.
*   [`solution`](https://github.com/iht/splittable-dofns-python/tree/solution): Reference branch containing the complete solutions. Use this to verify your work.

---

## 🛠️ Setup & Dependencies

1.  **Python Version:** Ensure you have Python 3.10, 3.11, 3.12, or 3.13 installed.
2.  **Install Requirements:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Kafka (Optional):** To run the Kafka examples, you will need a running Kafka cluster. Instructions to set up a local Kafka cluster using Minikube are provided [below](#-running-kafka-locally-with-minikube).

---

## 🚀 Running the Synthetic Pipelines

These pipelines use synthetic data generators to demonstrate Splittable DoFns without external dependencies.

### 1. Batch Pipeline
Demonstrates reading files in parallel by chunks.
*   **Run command:**
    ```bash
    python my_batch_pipeline.py
    ```
*   **Implementation file:** `mydofns/synthetic_sdfn_batch.py`

### 2. Streaming Pipeline
Demonstrates a streaming source with multiple partitions.
*   **Run command:**
    ```bash
    python my_streaming_synth_pipeline.py
    ```
*   **Implementation file:** `mydofns/synthetic_sdfn_streaming.py`
*   *Note: You can configure the number of partitions (default is 4) in `mydofns/synthetic_sdfn_streaming.py` (around line 62).*

---

## 🎡 Running the Kafka Pipelines

To run these examples, you need to set up a Kafka cluster and populate a topic.

### 🐳 Running Kafka Locally with Minikube

Follow these steps to set up Kafka in a local Minikube cluster:

1.  **Install Minikube:** Follow the [Minikube Start Guide](https://minikube.sigs.k8s.io/docs/start/).
2.  **Configure Access:** Set up an alias for convenience:
    ```bash
    alias k=kubectl
    ```
3.  **Create Namespace:**
    ```bash
    k create namespace kafka
    ```
4.  **Install Strimzi Kafka Operator:**
    ```bash
    k create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
    ```
5.  **Deploy Ephemeral Kafka Cluster:**
    ```bash
    k apply -f manifests/kafka-cluster.yaml -n kafka
    ```
6.  **Retrieve Kafka Bootstrap Server Details:**
    *   Get Node Port:
        ```bash
        k get service my-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}' -n kafka
        ```
    *   Get Minikube IP:
        ```bash
        k get node minikube -o=jsonpath='{range .status.addresses[*]}{.type}{"\t"}{.address}{"\n"}'
        ```
    *   Define the bootstrap server environment variable (replace with your IP and Port):
        ```bash
        export BOOTSTRAP="<MINIKUBE_IP>:<NODE_PORT>"
        ```

### 📝 Topic Creation & Data Population

Use the helper script `kafka_single_client.py` to manage the Kafka topic:

1.  **Create Topic:**
    ```bash
    python kafka_single_client.py --bootstrap $BOOTSTRAP --create
    ```
2.  **Produce Test Data:**
    ```bash
    python kafka_single_client.py --bootstrap $BOOTSTRAP
    ```
3.  **Verify Data (Consume):**
    ```bash
    python kafka_single_client.py --consumer --bootstrap $BOOTSTRAP
    ```

### 🏃 Running the Kafka Pipeline

*   **Run command:**
    ```bash
    python my_streaming_kafka_pipeline.py --bootstrap $BOOTSTRAP
    ```
*   **Implementation file:** `mydofns/kafka_sdfn_streaming.py`
*   *Note: Ensure the partition count matches the one used in `kafka_single_client.py`.*