package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Message 定义对象结构，与客户端一致
type Message struct {
	Text string `msgpack:"text"`
	ID   int    `msgpack:"id"`
}

// Server 定义服务端结构
type Server struct {
	socketPath string
	listener   net.Listener
	wg         sync.WaitGroup
	quit       chan os.Signal
}

// NewServer 创建一个新的 Server 实例
func NewServer(socketPath string) *Server {
	return &Server{
		socketPath: socketPath,
		quit:       make(chan os.Signal, 1),
	}
}

// Start 启动服务器，监听 Unix Socket
func (s *Server) Start() error {
	// 如果 Socket 文件已存在，删除它
	if _, err := os.Stat(s.socketPath); err == nil {
		if err := os.Remove(s.socketPath); err != nil {
			return fmt.Errorf("无法删除已存在的 socket 文件: %w", err)
		}
	}

	// 创建 Unix Socket 监听器
	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("无法监听 Unix Socket: %w", err)
	}
	s.listener = listener

	fmt.Printf("服务端已启动，监听 %s\n", s.socketPath)

	// 处理系统信号以实现优雅关闭
	signal.Notify(s.quit, syscall.SIGINT, syscall.SIGTERM)
	go s.handleShutdown()

	// 接受连接
	go s.acceptConnections()

	return nil
}

// acceptConnections 接受并处理传入的连接
func (s *Server) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// 检查错误是否由于监听器被关闭
			if errors.Is(err, net.ErrClosed) {
				fmt.Println("监听器已关闭，停止接受连接")
				return
			}

			select {
			case <-s.quit:
				// 服务器正在关闭
				return
			default:
				fmt.Printf("接受连接失败: %v\n", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个连接
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("新连接来自: %s\n", clientAddr)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute)) // 根据需要调整

		// 读取长度前缀
		lengthBytes := make([]byte, 4)
		_, err := io.ReadFull(reader, lengthBytes)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				fmt.Printf("连接超时: %s\n", clientAddr)
			} else if err == io.EOF {
				fmt.Printf("连接关闭: %s\n", clientAddr)
			} else {
				fmt.Printf("读取长度前缀失败 (%s): %v\n", clientAddr, err)
			}
			return
		}

		msgLength := binary.BigEndian.Uint32(lengthBytes)
		if msgLength == 0 {
			fmt.Printf("收到无效消息长度 (%s)\n", clientAddr)
			return
		}

		// 读取消息数据
		msgData := make([]byte, msgLength)
		_, err = io.ReadFull(reader, msgData)
		if err != nil {
			fmt.Printf("读取消息数据失败 (%s): %v\n", clientAddr, err)
			return
		}

		// 解码 MsgPack 数据
		var msg Message
		err = msgpack.Unmarshal(msgData, &msg)
		if err != nil {
			fmt.Printf("解码 MsgPack 失败 (%s): %v\n", clientAddr, err)
			return
		}

		fmt.Printf("收到消息 (%s): %+v\n", clientAddr, msg)

		// 处理消息（示例：将 ID 加 1，添加前缀）
		response := Message{
			Text: "服务器已收到: " + msg.Text,
			ID:   msg.ID + 1,
		}

		// 编码响应数据
		respData, err := msgpack.Marshal(response)
		if err != nil {
			fmt.Printf("编码 MsgPack 失败 (%s): %v\n", clientAddr, err)
			return
		}

		// 构造响应消息
		respLength := uint32(len(respData))
		respLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(respLengthBytes, respLength)
		fullResp := append(respLengthBytes, respData...)

		// 写入响应数据
		_, err = writer.Write(fullResp)
		if err != nil {
			fmt.Printf("写入数据失败 (%s): %v\n", clientAddr, err)
			return
		}

		// 刷新缓冲区，确保数据被发送
		err = writer.Flush()
		if err != nil {
			fmt.Printf("刷新缓冲区失败 (%s): %v\n", clientAddr, err)
			return
		}
	}
}

// handleShutdown 处理优雅关闭
func (s *Server) handleShutdown() {
	<-s.quit
	fmt.Println("\n服务端正在关闭...")

	// 关闭监听器，停止接受新连接
	if err := s.listener.Close(); err != nil {
		fmt.Printf("关闭监听器失败: %v\n", err)
	}

	// 等待所有连接处理完成
	s.wg.Wait()

	// 删除 Socket 文件
	if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
		fmt.Printf("删除 socket 文件失败: %v\n", err)
	}

	fmt.Println("服务端已关闭。")
	os.Exit(0)
}

func main() {
	socketPath := "/tmp/dba.sock"

	// 创建并启动服务器
	server := NewServer(socketPath)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "启动服务端失败: %v\n", err)
		os.Exit(1)
	}

	// 阻塞主 Goroutine，直到收到关闭信号
	select {}
}
