// main.go
package main

import (
	"fmt"
	ccc "github.com/iamdanielyin/mod-boot-go/cmd"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
)

// DBAProcess manages the dba subprocess
type DBAProcess struct {
	cmd    *exec.Cmd
	doneCh chan error
}

// Start launches the dba subprocess
func (d *DBAProcess) Start() error {
	d.cmd = exec.Command("dba")

	// Optional: Redirect subprocess's stdout and stderr to main process's stdout and stderr
	d.cmd.Stdout = os.Stdout
	d.cmd.Stderr = os.Stderr

	// Set the subprocess to run in a new process group
	d.cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Start the subprocess
	if err := d.cmd.Start(); err != nil {
		return err
	}

	log.Printf("dba subprocess started with PID: %d", d.cmd.Process.Pid)

	// Monitor the subprocess exit
	d.doneCh = make(chan error, 1)
	go func() {
		err := d.cmd.Wait()
		log.Printf("dba subprocess Wait() returned: %v", err)
		d.doneCh <- err
	}()

	return nil
}

// Stop terminates the dba subprocess gracefully
func (d *DBAProcess) Stop() error {
	if d.cmd == nil || d.cmd.Process == nil {
		return nil
	}

	log.Println("Terminating dba subprocess...")

	// Get the process group ID
	pgid, err := syscall.Getpgid(d.cmd.Process.Pid)
	if err != nil {
		log.Printf("Failed to get pgid: %v", err)
		return err
	}

	// Send SIGTERM to the entire process group
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
		log.Printf("Failed to send SIGTERM to dba subprocess group: %v", err)
		return err
	}

	// Wait for the subprocess to exit, with a timeout
	select {
	case err := <-d.doneCh:
		if err != nil {
			log.Printf("dba subprocess exited with error: %v", err)
			return err
		}
		log.Println("dba subprocess exited gracefully")
	case <-time.After(10 * time.Second): // Increased timeout
		log.Println("dba subprocess did not exit in time, killing it")
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
			log.Printf("Failed to kill dba subprocess group: %v", err)
			return err
		}
	}

	return nil
}

func main() {
	// 创建一个实例
	dba := &DBAProcess{}

	// 启动子进程
	if err := dba.Start(); err != nil {
		log.Fatalf("Failed to start dba subprocess: %v", err)
	}
	defer func() {
		if err := dba.Stop(); err != nil {
			log.Printf("Error terminating dba subprocess: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)
	socketPath := "/tmp/dba.sock"
	poolSize := 1 // 根据实际需求设置连接池大小

	// 创建 UnixClient 实例
	client, err := ccc.NewUnixClient(socketPath, poolSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 UnixClient 失败: %v\n", err)
		return
	}
	defer client.Close()

	// 创建 Fiber 应用
	app := fiber.New()

	// 定义 /ping API 端点
	app.Get("/ping", func(c fiber.Ctx) error {
		// 准备发送的消息
		msg := ccc.Message{
			Text: fmt.Sprintf("Hello, server! #%d", 1),
			ID:   1,
		}

		var respMsg ccc.Message

		err := client.UnixClientCall(msg, &respMsg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "UnixClientCall 失败: %v\n", err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to send data to dba")
		}

		fmt.Printf("收到响应 #%d: %+v\n", 1, respMsg)
		return c.JSON(&respMsg)
	})

	// 服务器错误通道
	serverErr := make(chan error, 1)

	// 启动 Fiber 服务器
	go func() {
		if err := app.Listen(":3000"); err != nil {
			serverErr <- err
		}
	}()
	log.Println("Fiber server started and listening on port 3000")

	// 信号捕获
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号或服务器错误
	select {
	case sig := <-sigs:
		log.Printf("Received signal: %s. Initiating shutdown...", sig)
	case err := <-serverErr:
		log.Fatalf("Server encountered an error: %v", err)
	}

	// 优雅关闭 Fiber 服务器
	shutdownDone := make(chan struct{})
	go func() {
		if err := app.Shutdown(); err != nil {
			log.Printf("Error during server shutdown: %v", err)
		} else {
			log.Println("Fiber server shut down successfully")
		}
		close(shutdownDone)
	}()

	// 设置服务器关闭超时
	select {
	case <-shutdownDone:
		// 服务器已关闭
	case <-time.After(10 * time.Second):
		log.Println("Server shutdown timed out, forcing exit")
	}

	// `defer` 已确保子进程被终止
	log.Println("Main program has exited")
}
