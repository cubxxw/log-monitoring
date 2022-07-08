package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

type Reader interface {
	Read(rc chan []byte)
}
type Writer interface {
	Writer(wc chan *Message)
}

type ReadFromFile struct {
	path string //读取文件的路径
}

type WriteToInfluxDB struct {
	influxDBsn string //写入的信息
}

type LogProcess struct {
	rc chan []byte   //多个goroutine之间的数据同步和通信（channels)
	wc chan *Message //写入模块同步数据
	//系统分为三个模块
	// + 实时读取  -- 文件路径
	// + 解析
	// + 写入  -- 写入的时候需要
	read  Reader //接口定义
	write Writer
}

type Message struct {
	//使用结构体来存储提取出来的监控数据
	TimeLocal                    time.Time //时间
	BytesSent                    int       //流量
	Path, Method, Scheme, Status string    //请求路径
	UpstreamTime, RequestTime    float64   //监控数据
}

func (r *ReadFromFile) Read(rc chan []byte) {
	//读取模块
	//打开文件
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}

	//从文件末尾开始逐行读取文件内容
	f.Seek(0, 2)
	rd := bufio.NewReader(f) //对f封装，此时rd就具备更多的方法
	for {
		line, err := rd.ReadBytes('\n') //读取文件内容直到遇见'\n'为止
		if err == io.EOF {
			//如果读取到末尾，此时应该等待新的
			time.Sleep(500 * time.Microsecond)
			continue
		} else if err != nil {
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}
		rc <- line[:len(line)-1] //数据的流向
		//去掉最后的换行符，此时我们可以用切片，从前往后，最后一位换行符-1去掉就好了
	}
}

func (l *LogProcess) Process() {
	//解析模块
	/* 输入数据：
	[07/July/2022:18:01:41 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "-"
	"KeepAliveClient" "-" 1.005 1.854
	*/
	//处理数据--将data中的所有字符修改为其大写格式。对于非ASCII字符，它的大写格式需要查表转换
	r := regexp.MustCompile(`\[([^\]]+)\]\s+(.*?)\s+\"(.*?)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+
	)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	loc, _ := time.LoadLocation("Asia/Shanghai") //我们用的是上海时区
	for v := range l.rc {
		ret := r.FindStringSubmatch(string(v)) //匹配数据内容，正则括号内容匹配到返回到
		//fmt.Println(ret)
		if len(ret) != 14 { //正则表达式有十三个括号
			log.Println("FindStringSubmatch fail:", string(v))
			continue //继续下一次匹配
		}

		message := &Message{}
		t, err := time.ParseInLocation("09/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			panic(fmt.Sprintf("parseninlocation error:%s", ret[4]))
		}

		message.TimeLocal = t

		byteSent, _ := strconv.Atoi(ret[5]) //将string类型转化为int
		// if err != nil {
		// 	panic(fmt.Sprintf("parseninlocation error:%s", err.Error(), ret[4]))
		// }
		message.BytesSent = byteSent

		//第六个括号匹配的是GET /foo?query=t HTTP/1.0
		reqSli := strings.Split(ret[3], " ") //按照空格切割第六个字段
		if len(reqSli) != 3 {
			log.Println("strings.split fail", ret[6]) //长度不是3说明报错了
			continue
		}

		message.Method = reqSli[0]

		u, err := url.Parse(reqSli[1])
		if err != nil {
			log.Println("url parse fail", ret[1])
			continue
		}
		message.Path = u.Path //此时可以直接从结构体中取到path

		message.Scheme = ret[5] //HTTP/1.0协议可以直接赋值给mess
		message.Status = ret[7]

		//UpstreamTime, RequestTime    float64   //监控数据  1.005 1.854在十二到十三
		upstreamTime, err := strconv.ParseFloat(ret[12], 64) //转化为float64
		if err != nil {
			fmt.Println("err = ", err, ret[1])
			continue
		}
		requestTime, err := strconv.ParseFloat(ret[13], 64) //转化为float64
		if err != nil {
			fmt.Println("err = ", err, ret[1])
			continue
		}
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime

		l.wc <- message //data是byte类型，需要转化为string类型
	}
}

func (w *WriteToInfluxDB) Writer(wc chan *Message) {
	//写入模块
	infSli := strings.Split(w.influxDBsn, "@") //使用@做切割

	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     infSli[0], //地址
		Username: infSli[1], //用户名
		Password: infSli[2], //密码
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  infSli[3],
		Precision: infSli[4],
	})
	if err != nil {
		log.Fatal(err)
	}

	for v := range wc {
		// 循环的写入数据
		/*
			+ Tags：Path, Method, Scheme, Status
			+ Fields：UpstreamTime, RequestTime，BytesSent
			+ Time：TimeLocal
		*/
		tags := map[string]string{"Path": v.Path, "Method": v.Method, "Scheme": v.Scheme, "Status": v.Status}
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime":  v.RequestTime,
			"BytesSent":    v.BytesSent,
		}

		pt, err := client.NewPoint("nginx_log", tags, fields, time.Now()) //创建Influxdb字段
		if err != nil {
			log.Fatal("client.NewPoint err = ", err)
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			log.Fatal(err)
		}

		// Close client resources
		if err := c.Close(); err != nil {
			log.Fatal(err)
		}

		log.Println("write success") //如果写入成功就打印日志
	}
}

func main() {
	var path, influDsn string
	flag.StringVar(&path, "path", "./access.log", "read file path") //"帮助信息"
	flag.StringVar(&influDsn, "influxDsn", "http://127.0.0.1:8086@myself@myselfpass@myself@s", "influx data source")

	flag.Parse() //解析参数
	r := &ReadFromFile{
		path: path,
	}
	w := &WriteToInfluxDB{
		influxDBsn: influDsn,
	}
	logprocess := &LogProcess{
		rc:    make(chan []byte),
		wc:    make(chan *Message),
		read:  r,
		write: w,
	}

	//使用goroutinue提高程序的性能
	go logprocess.read.Read(logprocess.rc)    //调用读取模块
	go logprocess.Process()                   //调用解析模块
	go logprocess.write.Writer(logprocess.wc) //调用写入模块

	//程序执行完后就自动退出了,需要等待
	time.Sleep(330 * time.Second)
}
