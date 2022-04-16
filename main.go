package main

import (
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"
    "log"
    "github.com/Shopify/sarama"
)

func main() 
	producer, err := newProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}

        {
    keypair, err := tls.LoadX509KeyPair("accesscert.cert", "accesskey.key")
    if err != nil {
        log.Println(err)
        return
    }

    caCert, err := ioutil.ReadFile("cacert.ca")
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
    brokers := []string{"kafka-heroku-go-alanscott-ff29.aivencloud.com:17651"}
    producer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        panic(err)
    }

    // produce logic would go here

    producer.Close();
}
