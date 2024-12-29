package ccc

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Message 定义对象结构
type Message struct {
	Text string `msgpack:"text"`
	ID   int    `msgpack:"id"`
}

// UnixClient 定义 Unix 客户端结构，包含连接池
type UnixClient struct {
	socketPath string
	pool       chan net.Conn
	mu         sync.Mutex
}

// NewUnixClient 创建一个新的 UnixClient，初始化连接池
func NewUnixClient(socketPath string, poolSize int) (*UnixClient, error) {
	client := &UnixClient{
		socketPath: socketPath,
		pool:       make(chan net.Conn, poolSize),
	}

	// 预先创建连接池中的连接
	for i := 0; i < poolSize; i++ {
		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			// 关闭已创建的连接
			client.Close()
			return nil, fmt.Errorf("初始化连接池失败: %w", err)
		}
		client.pool <- conn
	}

	return client, nil
}

// Close 关闭连接池中的所有连接
func (c *UnixClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.pool)
	for conn := range c.pool {
		conn.Close()
	}
}

// UnixClientCall 通过 Unix Socket 发送请求并接收响应，使用连接池和 MsgPack
// 参数：
// - input: 要发送的数据，可以是结构体或 map。
// - dst: 接收响应的数据的指针。
// 返回值：
// - error: 如果过程中发生错误，则返回错误信息。
func (c *UnixClient) UnixClientCall(input interface{}, dst interface{}) error {
	// 从连接池获取一个连接
	var conn net.Conn
	select {
	case conn = <-c.pool:
		// 获取到连接
	default:
		// 连接池已满，尝试新建连接
		var err error
		conn, err = net.Dial("unix", c.socketPath)
		if err != nil {
			return fmt.Errorf("获取连接失败: %w", err)
		}
	}

	// 确保连接被归还到池中
	defer func() {
		if conn != nil {
			select {
			case c.pool <- conn:
				// 连接成功归还
			default:
				// 连接池已满，关闭连接
				conn.Close()
			}
		}
	}()

	// 分别设置写入和读取的超时
	writeDeadline := time.Now().Add(5 * time.Second)
	if err := conn.SetWriteDeadline(writeDeadline); err != nil {
		conn.Close()
		return fmt.Errorf("设置写入超时失败: %w", err)
	}

	readDeadline := time.Now().Add(5 * time.Second)
	if err := conn.SetReadDeadline(readDeadline); err != nil {
		conn.Close()
		return fmt.Errorf("设置读取超时失败: %w", err)
	}

	// 使用缓冲读写器
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	// 将输入数据编码为 MsgPack
	data, err := msgpack.Marshal(input)
	if err != nil {
		return fmt.Errorf("MsgPack 编码失败: %w", err)
	}

	// 构造消息：长度前缀 + MsgPack 数据
	msgLength := uint32(len(data))
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, msgLength)
	fullMsg := append(lengthBytes, data...)

	// 写入数据到缓冲区
	_, err = writer.Write(fullMsg)
	if err != nil {
		conn.Close()
		conn = nil
		return fmt.Errorf("写入到 Socket 失败: %w", err)
	}

	// 刷新缓冲区，确保数据被发送
	err = writer.Flush()
	if err != nil {
		conn.Close()
		conn = nil
		return fmt.Errorf("刷新写入器失败: %w", err)
	}

	// 从 Socket 读取响应数据
	// 先读取长度前缀
	respLengthBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, respLengthBytes)
	if err != nil {
		conn.Close()
		conn = nil
		return fmt.Errorf("读取响应长度前缀失败: %w", err)
	}
	respLength := binary.BigEndian.Uint32(respLengthBytes)

	if respLength == 0 {
		conn.Close()
		return fmt.Errorf("收到无效响应长度")
	}

	// 读取响应消息数据
	respData := make([]byte, respLength)
	_, err = io.ReadFull(reader, respData)
	if err != nil {
		conn.Close()
		conn = nil
		return fmt.Errorf("读取响应数据失败: %w", err)
	}

	// 解码响应数据
	err = msgpack.Unmarshal(respData, dst)
	if err != nil {
		return fmt.Errorf("解码响应 MsgPack 失败: %w", err)
	}

	return nil
}

func main() {
	socketPath := "/tmp/dba.sock"
	poolSize := 10 // 根据实际需求设置连接池大小

	// 创建 UnixClient 实例
	client, err := NewUnixClient(socketPath, poolSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 UnixClient 失败: %v\n", err)
		return
	}
	defer client.Close()

	// 使用 WaitGroup 模拟高并发调用
	var wg sync.WaitGroup
	concurrentRequests := 100

	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 准备发送的消息
			msg := Message{
				Text: fmt.Sprintf("Hello, server! #%d", id),
				ID:   id,
			}

			var respMsg Message

			err := client.UnixClientCall(msg, &respMsg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "UnixClientCall 失败: %v\n", err)
				return
			}

			fmt.Printf("收到响应 #%d: %+v\n", id, respMsg)
		}(i)
	}

	wg.Wait()
}
