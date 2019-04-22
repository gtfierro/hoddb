package loader

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"

	//"github.com/op/go-logging"
	logrus "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	Database struct {
		Path       string
		Buildings  map[string]string
		Ontologies []string
	}

	Output struct {
		LogLevel string
	}

	Http struct {
		Enable  bool
		Address string
		Port    string
	}

	Grpc struct {
		Enable  bool
		Address string
		Port    string
	}

	Profile struct {
		EnableCpu   bool
		EnableMem   bool
		EnableBlock bool
		EnableHttp  bool
		HttpPort    string
	}
}

func init() {
	prefix := os.Getenv("GOPATH")
	// switch prefix to default GOPATH /home/{user}/go
	if prefix == "" {
		u, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		prefix = filepath.Join(u.HomeDir, "go")
	}
	// set defaults for config

	// Database
	viper.SetDefault("Database.Path", "_hod_")
	viper.SetDefault("Database.Buildings", make(map[string]string))
	viper.SetDefault("Database.Ontologies", []string{
		prefix + "/src/github.com/gtfierro/hod/BrickFrame.ttl",
		prefix + "/src/github.com/gtfierro/hod/Brick.ttl",
		prefix + "/src/github.com/gtfierro/hod/BrickUse.ttl",
		prefix + "/src/github.com/gtfierro/hod/BrickTag.ttl",
	})

	// GRPC Interface
	viper.SetDefault("Grpc.Enable", true)
	viper.SetDefault("Grpc.Address", "localhost")
	viper.SetDefault("Grpc.Port", "47808")

	// Profile
	viper.SetDefault("Profile.EnableCpu", false)
	viper.SetDefault("Profile.EnableMem", false)
	viper.SetDefault("Profile.EnableBlock", false)
	viper.SetDefault("Profile.EnableHttp", false)
	viper.SetDefault("Profile.HttpPort", "8080")

}

func getCfg() *Config {
	//level, err := logging.LogLevel(viper.GetString("LogLevel"))
	//if err != nil {
	//	level = logging.DEBUG
	//}

	level, err := logrus.ParseLevel(viper.GetString("Output.LogLevel"))
	if err != nil {
		level = logrus.DebugLevel
	}
	logrus.SetLevel(level)

	cfg := &Config{}
	cfg.Database.Path = viper.GetString("Database.Path")
	cfg.Database.Buildings = viper.GetStringMapString("Database.Buildings")
	cfg.Database.Ontologies = viper.GetStringSlice("Database.Ontologies")

	cfg.Http.Enable = viper.GetBool("Http.Enable")
	cfg.Http.Address = viper.GetString("Http.Address")
	cfg.Http.Port = viper.GetString("Http.Port")

	cfg.Grpc.Enable = viper.GetBool("Grpc.Enable")
	cfg.Grpc.Address = viper.GetString("Grpc.Address")
	cfg.Grpc.Port = viper.GetString("Grpc.Port")

	cfg.Profile.EnableCpu = viper.GetBool("Profile.EnableCpu")
	cfg.Profile.EnableMem = viper.GetBool("Profile.EnableMem")
	cfg.Profile.EnableBlock = viper.GetBool("Profile.EnableBlock")
	cfg.Profile.EnableHttp = viper.GetBool("Profile.EnableHttp")
	cfg.Profile.HttpPort = viper.GetString("Profile.HttpPort")
	return cfg
}

func ReadConfig(file string) (*Config, error) {
	if len(file) > 0 {
		viper.SetConfigFile(file)
	}
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}
	viper.AutomaticEnv()

	return getCfg(), nil
}

func ReadConfigFromString(configString string) (*Config, error) {
	viper.SetConfigType("yaml")
	if err := viper.ReadConfig(strings.NewReader(configString)); err != nil {
		return nil, err
	}
	viper.AutomaticEnv()

	return getCfg(), nil
}
