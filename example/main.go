package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"

	"github.com/skeletongo/rabbitmq"
)

const Addr = "amqp://guest:guest@127.0.0.1:5672/"

var RMQ = rabbitmq.NewRabbitMQ(Addr)

func main() {
	var err error

	// 模拟数据源
	messages := make(chan []byte)
	go func() {
		i := 0
		for {
			i++
			messages <- []byte(fmt.Sprint(i))
			time.Sleep(time.Second)
		}
	}()

	// 发送端
	if err = RMQ.AddAction(func(mq *rabbitmq.RabbitMQ) error {
		log.Println("do pub...")
		closeCh := mq.Conn.NotifyClose(make(chan *amqp.Error, 1))
		blockCh := mq.Conn.NotifyBlocked(make(chan amqp.Blocking, 1))

		for {
			ch := mq.GetChannel(mq.Conn)
			if ch == nil {
				return nil
			}

			// 模拟网络或通道断开的情况
			//time.AfterFunc(time.Second*5, func() {
			//	if rand.Intn(2) == 0 {
			//		mq.Conn.Close()
			//	} else {
			//		ch.Close()
			//	}
			//})

			var (
				closeChannelCh = ch.NotifyClose(make(chan *amqp.Error, 1))
				flowCh         = ch.NotifyFlow(make(chan bool, 1))
				cancelCh       = ch.NotifyCancel(make(chan string, 1))
				returnCh       = ch.NotifyReturn(make(chan amqp.Return, 1))
				confirm        = make(chan amqp.Confirmation, 1)
				reading        = messages
			)
			if err := ch.Confirm(false); err != nil {
				log.Printf("publisher confirms not supported")
				continue
			} else {
				ch.NotifyPublish(confirm)
			}

			err = ch.ExchangeDeclare(
				"e.pub",
				amqp.ExchangeFanout,
				false,
				true,
				false,
				false,
				nil)
			if err != nil {
				log.Println("declare exchange error:", err)
				continue
			}

		pub:
			for {
				select {
				case body, ok := <-reading:
					if !ok {
						return nil
					}

					// 测试安全关闭, 消息“10”应该可以正常发送
					//if string(body) == "10" {
					//	mq.Close()
					//}

					if err = ch.Publish(
						"e.pub",
						"",
						true,
						false,
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        body,
						}); err != nil {
						log.Println("publish error:", err)
						//todo 发送失败
					} else {
						// 已发送，等待确认
						confirmed, ok := <-confirm
						if !ok {
							log.Printf("nack confirm body:%v\n", string(body))
							//todo 发送失败,此时消息是否被服务端收到是不确定的
							break pub
						}
						if !confirmed.Ack {
							log.Printf("nack confirm (%v) body:%v\n", confirmed.DeliveryTag, string(body))
							//todo 发送失败
						} else {
							// 发送成功
							log.Println("send success:", string(body))
						}
					}

				case block, ok := <-blockCh:
					if !ok {
						return nil
					}
					if block.Active {
						reading = nil
					} else {
						reading = messages
					}

				case active, ok := <-flowCh:
					if !ok {
						break pub
					}
					if active {
						reading = messages
					} else {
						reading = nil
					}

				case data, ok := <-returnCh:
					if !ok {
						break pub
					}
					log.Printf("return: %v\n", string(data.Body))
					//todo 发送失败

				case data, ok := <-cancelCh:
					if !ok {
						break pub
					}
					log.Printf("cancel: %v\n", data)
					//todo 消费者异常关闭

				case data := <-closeChannelCh:
					if data != nil {
						log.Printf("channel close:%+v\n", *data)
					}
					break pub

				case <-closeCh:
					return nil

				case <-mq.Closing:
					return nil
				}
			}
		}
	}, true); err != nil {
		log.Println("add product error:", err)
	}

	// 接收端
	if err = RMQ.AddAction(func(mq *rabbitmq.RabbitMQ) error {
		log.Println("do sub...")
		closeCh := mq.Conn.NotifyClose(make(chan *amqp.Error, 1))

		for {
			ch := mq.GetChannel(mq.Conn)
			if ch == nil {
				return nil
			}
			closeChannelCh := ch.NotifyClose(make(chan *amqp.Error, 1))

			q, err := ch.QueueDeclare(
				"",
				false,
				true,
				false,
				false,
				nil)
			if err != nil {
				log.Println("declare queue error:", err)
				continue
			}

			err = ch.QueueBind(q.Name, "", "e.pub", false, nil)
			if err != nil {
				log.Println("queue bind error:", err)
				continue
			}

			msg, err := ch.Consume(
				q.Name,
				"",
				false,
				false,
				false,
				false,
				nil)
			if err != nil {
				log.Println("consume error:", err)
				continue
			}

		pub:
			for {
				select {
				case delivery, ok := <-msg:
					if !ok {
						break pub
					}
					log.Println("receiver message:", string(delivery.Body))

					// 测试安全关闭, 消息“10”应该可以正常消费
					//if string(delivery.Body) == "10" {
					//	mq.Close()
					//}

					//todo 收到消息,模拟消息处理需要耗时
					time.Sleep(time.Second * 2)
					delivery.Ack(false)
					log.Println("receiver finish:", string(delivery.Body))

				case <-closeChannelCh:
					break pub

				case <-closeCh:
					return nil

				case <-mq.Closing:
					return nil
				}
			}
		}
	}, true); err != nil {
		log.Println("add consume error:", err)
	}

	// 等待关闭
	<-RMQ.Closed
}
