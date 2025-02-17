# pulsar-client-go

[English](README.md) | [中文版](README_cn.md)

## 使用前准备

1. AccessID：由涂鸦平台提供
2. AccessKey：由涂鸦平台提供
3. pulsar 地址：根据不同的业务区域选择 Pulsar 地址。可以从涂鸦对接文档中查询获取。

## Example

```
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	pulsar "github.com/pepper-iot/tuya-pulsar-sdk-go"
	"github.com/pepper-iot/tuya-pulsar-sdk-go/pkg/tylog"
	"github.com/pepper-iot/tuya-pulsar-sdk-go/pkg/tyutils"
	"github.com/sirupsen/logrus"
)

func main() {
	pulsar.SetInternalLogLevel(logrus.DebugLevel)
	tylog.SetGlobalLog("sdk", false)
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
		Topic: topic,
		Auth:  pulsar.NewAuthProvider(accessID, accessKey),
	}
	csm, _ := c.NewConsumer(csmCfg)

	// handle message
	csm.ReceiveAndHandle(context.Background(), &helloHandler{AesSecret: accessKey[8:24]})

	time.Sleep(10 * time.Second)
}

type helloHandler struct {
	AesSecret string
}

func (h *helloHandler) HandlePayload(ctx context.Context, msg *pulsar.Message, payload []byte) error {
	tylog.Info("payload preview", tylog.String("payload", string(payload)))

	// let's decode the payload with AES
	m := map[string]interface{}{}
	err := json.Unmarshal(payload, &m)
	if err != nil {
		tylog.Error("json unmarshal failed", tylog.ErrorField(err))
		return nil
	}
	bs := m["data"].(string)
	de, err := base64.StdEncoding.DecodeString(string(bs))
	if err != nil {
		tylog.Error("base64 decode failed", tylog.ErrorField(err))
		return nil
	}
	decode := tyutils.EcbDecrypt(de, []byte(h.AesSecret))
	tylog.Info("aes decode", tylog.ByteString("decode payload", decode))

	return nil
}


```

## 注意事项

1. 确保 accessID，accessKey 是正确的
2. 确保 pulsar 地址是正确的，应该使用 pulsar://mqe.tuyaus.com:7285 模式而不是
   pulsar+ssl://mqe.tuyaus.com:7285
3. 尽量确保你使用的 sdk 代码版本是最新的

## About debug

通过下面的代码，你可以在终端看到所有和 pulsar 服务的通信

```
func main(){
	pulsar.SetInternalLogLevel(logrus.DebugLevel)
	// other code
}
```

通过下面的代码，你可以看到 tuya_pulsar_go_sdk 的日志信息。
与此同时，日志会保存在 logs/sdk.log 文件中

```
func main(){
	tylog.SetGlobalLog("sdk", false)
}
```

在正式环境，你可能不希望 sdk 日志都输出在终端，此时建议你使用下面的代码。
将日志只输出到文件中。

```
func main(){
	tylog.SetGlobalLog("sdk", true)
}
```

## 技术支持

你可以通过以下方式获得 Tua 开发者技术支持：

- 涂鸦帮助中心: [https://support.tuya.com/zh/help](https://support.tuya.com/zh/help)
- 涂鸦技术工单平台: [https://iot.tuya.com/council](https://iot.tuya.com/council)
