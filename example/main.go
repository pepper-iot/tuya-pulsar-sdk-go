package main

import (
	"context"
	"encoding/base64"
	"encoding/json"

	pulsar "github.com/pepper-iot/tuya-pulsar-sdk-go"
	"github.com/pepper-iot/tuya-pulsar-sdk-go/pkg/tyutils"
	"github.com/rs/zerolog/log"
	"github.com/tuya/pulsar-client-go/core/manage"
)

func main() {
	accessID := "accessID"
	accessKey := "accessKey"
	topic := pulsar.TopicForAccessID(accessID)

	// create client
	cfg := pulsar.ClientConfig{
		PulsarAddr: pulsar.PulsarAddrCN,
	}
	c := pulsar.NewClient(cfg)

	// create consumer
	csmCfg := pulsar.ConsumerConfig{
		Topic:            topic,
		Auth:             pulsar.NewAuthProvider(accessID, accessKey),
		SubscriptionMode: manage.SubscriptionModeShard,
	}
	csm, _ := c.NewConsumer(csmCfg)

	// handle message
	csm.ReceiveAndHandle(context.Background(), &helloHandler{AesSecret: accessKey[8:24]})
}

type helloHandler struct {
	AesSecret string
}

func (h *helloHandler) HandlePayload(ctx context.Context, msg *pulsar.Message, payload []byte) error {
	log.Info().Str("payload", string(payload)).Msg("payload preview")

	// let's decode the payload with AES
	m := map[string]interface{}{}
	err := json.Unmarshal(payload, &m)
	if err != nil {
		log.Error().Err(err).Msg("json unmarshal failed")
		return nil
	}
	bs := m["data"].(string)
	de, err := base64.StdEncoding.DecodeString(string(bs))
	if err != nil {
		log.Error().Err(err).Msg("base64 decode failed")
		return nil
	}
	decode := tyutils.EcbDecrypt(de, []byte(h.AesSecret))
	log.Info().Str("decode", string(decode)).Msg("decode payload")

	return nil
}
