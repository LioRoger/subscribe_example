package kfkdts

// Config ...
type Config struct {
	IsAliyunDTS bool
	User        string
	Passwd      string
	Hosts       []string
	Topic       string
	GroupID     string
	Partition   int32
	Offset      int64 // -1:OffsetNewest -2:OffsetOldest
}

// NewConfig ...
//func NewConfig() (config *Config, err error) {
//	var cfg struct {
//		Client *Config
//	}
//	if err = toml.DecodeFile("kafka.toml", &cfg); err != nil {
//		return nil, err
//	}
//	return cfg.Client, nil
//}
