package rabbitmq

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	// rabbitMQ重连时间间隔
	ReDialTime = 5 * time.Second
	// 通道重连时间间隔
	ReGetChannelTime = 5 * time.Second
	// Action队列容量
	ActionChannelCapacity = 1
)

// Action 自定义处理方法，是启动一个协程去执行的, 此方法还要注意是否需要保证幂等性，因为当网络连接断开重连后会重新执行
type Action func(mq *RabbitMQ) error

// RabbitMQ rabbitmq管理器
// 主要是实现断线重连和安全退出
type RabbitMQ struct {
	// 用来等待action协程退出
	sync.WaitGroup

	// Addr rabbitmq连接地址
	Addr string

	// Conn rabbitmq连接
	Conn *amqp.Connection

	// Closing 模块关闭中的标记
	Closing chan struct{}

	// Closed 模块已关闭的标记
	Closed chan struct{}

	// actionCh Action消息队列
	actionCh chan Action

	// actions 收到的Action对象集合
	actions []Action
}

// NewRabbitMQ 创建rabbitmq管理器
// addr rabbitmq服务连接地址
func NewRabbitMQ(addr string) *RabbitMQ {
	r := new(RabbitMQ)
	r.Addr = addr
	r.Closing = make(chan struct{})
	r.Closed = make(chan struct{})
	r.actionCh = make(chan Action, ActionChannelCapacity)
	go r.connect()
	return r
}

// Close 主动关闭此模块，可重复调用，但只有第一次有效
func (r *RabbitMQ) Close() {
	select {
	case <-r.Closed:
		return
	case <-r.Closing:
		return
	default:
		close(r.Closing)
		go func() {
			r.Wait()
			close(r.Closed)
		}()
	}
}

// IsClose 是否已经关闭
// 已关闭表示所有子协程(Action)已经退出
func (r *RabbitMQ) IsClose() bool {
	select {
	case <-r.Closed:
		return true
	default:
		return false
	}
}

// IsClosing 是否关闭中
// 关闭中，等待子协程(Action)退出
func (r *RabbitMQ) IsClosing() bool {
	if r.IsClose() {
		return false
	}
	select {
	case <-r.Closing:
		return true
	default:
		return false
	}
}

// connect 断线重连
func (r *RabbitMQ) connect() {
	var err error
here:
	for {
		if r.IsClose() {
			break
		}

		r.Conn, err = amqp.Dial(r.Addr)
		if err != nil {
			log.Println("rabbitmq dial error:", err)
			select {
			case <-r.Closed:
				break here
			case <-time.After(ReDialTime):
				continue
			}
		}

		r.run()
	}

	if r.Conn != nil || !r.Conn.IsClosed() {
		r.Conn.Close()
	}
}

// run 创建Action协程，监听r.Conn是否断开
func (r *RabbitMQ) run() {
	closeCh := r.Conn.NotifyClose(make(chan *amqp.Error, 1))

	r.Add(len(r.actions))
	for _, v := range r.actions {
		action := v
		go func() {
			defer r.Done()
			if err := action(r); err != nil {
				log.Println("rabbitmq action error:", err)
			}
		}()
	}

	for {
		select {
		case action := <-r.actionCh:
			// 需要执行的自定义消息
			r.Add(1)
			r.actions = append(r.actions, action)
			go func() {
				defer r.Done()
				if err := action(r); err != nil {
					log.Println("rabbitmq action error:", err)
				}
			}()

		case closeSign := <-closeCh:
			// 连接断开
			if closeSign != nil {
				log.Printf("rabbitmq connect close: %+v\n", *closeSign)
			}
			// 等待其它协程退出，然后重新启动任务协程
			r.Wait()
			return

		case <-r.Closed:
			return
		}
	}
}

// GetChannel 同步方式获取通道，并在网络连接断开或收到关闭信号时退出
func (r *RabbitMQ) GetChannel(conn *amqp.Connection) *amqp.Channel {
	if conn == nil || conn.IsClosed() {
		return nil
	}

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	var err error
	var ch *amqp.Channel
	for {
		select {
		case <-r.Closing:
			return nil
		case <-closeCh:
			return nil
		default:
		}

		log.Println("get channel ...")
		ch, err = conn.Channel()
		if err != nil {
			log.Println("rabbitmq get channel error:", err)
			select {
			case <-r.Closing:
				return nil
			case <-closeCh:
				return nil
			case <-time.After(ReGetChannelTime):
				continue
			}
		}

		return ch
	}
}

// AddAction 新增 Action
// sync true同步方式
// action 此方法还要注意是否需要保证幂等性，因为当网络连接断开重连后会重新执行
func (r *RabbitMQ) AddAction(action Action, sync bool) error {
	if r.IsClose() || r.IsClosing() {
		return errors.New("rabbitmq closing")
	}

	if sync {
		r.actionCh <- action
		return nil
	}

	select {
	case r.actionCh <- action:
		return nil
	default:
		return errors.New("add action blocking")
	}
}
