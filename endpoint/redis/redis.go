/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"net/textproto"
	"strings"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "redis"

// Endpoint 别名
type Endpoint = Redis

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage 请求消息
type RequestMessage struct {
	redisClient *redis.Client
	topic       string
	body        []byte
	msg         *types.RuleMsg
	err         error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.topic)
	return header
}

func (r *RequestMessage) From() string {
	return r.topic
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))

		ruleMsg.Metadata.PutValue("topic", r.From())

		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
	r.body = body
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage http响应消息
type ResponseMessage struct {
	redisClient *redis.Client
	topic       string
	body        []byte
	msg         *types.RuleMsg
	headers     textproto.MIMEHeader
	err         error
	log         func(format string, v ...interface{})
}

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return r.topic
}

func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.Headers().Get("topic")
	if topic != "" {
		_ = r.redisClient.Publish(context.Background(), topic, string(r.body))
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

type Config struct {
	// Redis服务器地址
	Server string
	// Redis密码
	Password string
	// Redis数据库index
	Db int
}

// Redis Redis接收端端点
type Redis struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config      Config
	redisClient *redis.Client
	pubSub      *redis.PubSub
	// 订阅映射关系，用于取消订阅
	subscriptions map[string]string
}

// Type 组件类型
func (n *Redis) Type() string {
	return Type
}

func (n *Redis) Id() string {
	return n.Config.Server
}

func (n *Redis) New() types.Node {
	return &Redis{
		Config: Config{
			Server: "127.0.0.1:6379",
			Db:     0,
		},
	}
}

// Init 初始化
func (n *Redis) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &n.Config)
	n.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (n *Redis) Destroy() {
	_ = n.Close()
}

func (n *Redis) Close() error {
	if nil != n.redisClient {
		_ = n.redisClient.Close()
	}
	n.BaseEndpoint.Destroy()
	return nil
}

func (n *Redis) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	// 初始化NATS客户端
	if err := n.initRedisClient(); err != nil {
		return "", err
	}

	routerId := router.GetId()
	if routerId == "" {
		routerId = router.GetFrom().ToString()
		router.SetId(routerId)
	}
	if n.pubSub != nil {
		_ = n.pubSub.Close()
	}
	channels := strings.Split(router.GetFrom().ToString(), ",")
	n.pubSub = n.redisClient.PSubscribe(context.Background(), channels...)

	go func() {
		// 遍历接收消息
		for msg := range n.pubSub.Channel() {
			exchange := &endpointApi.Exchange{
				In: &RequestMessage{
					redisClient: n.redisClient,
					topic:       msg.Channel,
					body:        []byte(msg.Payload),
				},
				Out: &ResponseMessage{
					redisClient: n.redisClient,
					topic:       msg.Channel,
					log: func(format string, v ...interface{}) {
						n.Printf(format, v...)
					},
				},
			}
			n.DoProcess(context.Background(), router, exchange)
			fmt.Println("Received message:", msg.Payload)
		}

	}()
	return routerId, nil
}

func (n *Redis) RemoveRouter(routerId string, params ...interface{}) error {
	if n.pubSub != nil {
		_ = n.pubSub.Close()
		n.pubSub = nil
	}
	return nil
}

func (n *Redis) Start() error {
	return n.initRedisClient()
}

// initRedisClient 初始化Redis客户端
func (n *Redis) initRedisClient() error {
	if n.redisClient == nil {
		n.redisClient = redis.NewClient(&redis.Options{
			Addr: n.Config.Server,
			DB:   n.Config.Db,
		})
		return n.redisClient.Ping(context.Background()).Err()
	}
	return nil
}

func (n *Redis) Printf(format string, v ...interface{}) {
	if n.RuleConfig.Logger != nil {
		n.RuleConfig.Logger.Printf(format, v...)
	}
}
