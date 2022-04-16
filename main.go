package main

import (
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"
    "log"
    "github.com/Shopify/sarama"
)

func main() {
    keypair, err := tls.LoadX509KeyPair("my-kafka-service.cert", "my-kafka-service.key")
    if err != nil {
        log.Println(err)
        return
    }

    caCert, err := ioutil.ReadFile("my-aiven-project-ca.pem")
    if err != nil {
        log.Println(err)
        return
    }
    caCertPool := x509.NewCertPool()
    caCertPool.AppendCertsFromPEM(caCert)

    tlsConfig := &tls.Config{
        Certificates: []tls.Certificate{keypair},
        RootCAs: caCertPool,
    }

    // init config, enable errors and notifications
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Net.TLS.Enable = true
    config.Net.TLS.Config = tlsConfig
    config.Version = sarama.V0_10_2_0

    // init producer
    brokers := []string{"my-kafka-service-my-aiven-project.aivencloud.com:12233"}
    producer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        panic(err)
    }

    // produce logic would go here

    producer.Close();
}
