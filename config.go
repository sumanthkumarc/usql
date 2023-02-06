package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Databases map[string]*DatabaseConfig `yaml:"databases"`
}

type DatabaseConfig struct {
	Name        string        `yaml:"name"`
	Host        string        `yaml:"host"`
	ReaderHost  string        `yaml:"reader_host"`
	Port        int           `yaml:"port"`
	DbType      string        `yaml:"db_type"`
	Credentials []*RoleConfig `yaml:"credentials"`
}

type RoleConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Name     string `yaml:"role"`
}

func (dc *DatabaseConfig) GetCreddentialsForRole(RoleName string) (RoleConfig, error) {

	// if no role name is provided, send the first one in list
	// rather than erroring out. Maybe assume a default role type?
	if RoleName == "" {
		return *dc.Credentials[0], nil
	}

	for _, role := range dc.Credentials {
		if role.Name == RoleName {
			return *role, nil
		}
	}

	return RoleConfig{}, fmt.Errorf("Role config doesn't exist for role %s in config file", RoleName)
}

var DBConfig Config
var DSN_STRING string = "DRIVER://USERNAME:PASSWORD@HOST/DATABASE"
var DB_CONFIG_DEFAULT_FILENAME = ".dbconfig.yaml"

func GetDsnForDB(databaseName string, args *Args) (string, error) {
	var roleCreds RoleConfig

	dbConfig, err := GetDatabaseConfig(databaseName, args)
	if err != nil {
		return "", err
	}

	// @todo sanity check for the config

	if args.Role != "" {
		roleCreds, err = dbConfig.GetCreddentialsForRole(args.Role)

		if err != nil {
			return "", err
		}
	}

	tokens := map[string]string{
		"DRIVER":   dbConfig.DbType,
		"USERNAME": roleCreds.Username,
		"PASSWORD": roleCreds.Password,
		// @todo change host based on role type. Ex - reader host for reader role
		"HOST":     dbConfig.Host,
		"DATABASE": dbConfig.Name,
	}

	return ReplaceTokens(DSN_STRING, tokens), nil
}

func GetDatabaseConfig(databaseName string, args *Args) (*DatabaseConfig, error) {
	configPath, err := DiscoverConfigPath(args)
	if err != nil {
		return &DatabaseConfig{}, err
	}

	readDatabaseConfig(configPath)

	if DBConfig.Databases[databaseName] == nil {
		return &DatabaseConfig{}, fmt.Errorf("Didn't find entry for %s database in config file at %s. Ensure entry exists under databases key in config file", databaseName, configPath)
	}

	return DBConfig.Databases[databaseName], nil
}

func DiscoverConfigPath(args *Args) (string, error) {
	var configPath string = args.ConfigFilePath

	if configPath != "" {
		if exist := CheckFileExistence(configPath); !exist {
			return "", fmt.Errorf("Unable to find the config file in given path %s", configPath)
		}
	} else {
		configPath = FindConfigFile()

		if configPath == "" {
			return "", fmt.Errorf("Unable to find the config file .dbconfig.yaml in current directory or in USQL_DB_CONFIG env var or at ~/.dbconfig.yaml")
		}
	}

	return configPath, nil
}

func FindConfigFile() string {
	var configPath string

	// try current directory
	configPath = DB_CONFIG_DEFAULT_FILENAME
	if exist := CheckFileExistence(configPath); exist {
		return configPath
	}

	// Search if the env var is set
	configPath, envSet := os.LookupEnv("USQL_DB_CONFIG")
	if envSet {
		if exist := CheckFileExistence(configPath); exist {
			return configPath
		}
	}

	// Search if the config is present at Home directory of current user
	usr, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stdout, "error: %v\n", err)
	}

	if usr.HomeDir != "" {
		configPath = filepath.Join(usr.HomeDir, DB_CONFIG_DEFAULT_FILENAME)
		if exist := CheckFileExistence(configPath); exist {
			return configPath
		}
	}

	return ""
}

func CheckFileExistence(filePath string) bool {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	} else if err != nil {
		return false
	} else {
		return true
	}
}

func readDatabaseConfig(configPath string) {

	path, _ := filepath.Abs(configPath)
	config, err := ioutil.ReadFile(path)

	if err != nil {
		log.Panicln(err)
	}

	err = yaml.Unmarshal(config, &DBConfig)

	if err != nil {
		log.Panicln(err)
	}
}

func ReplaceTokens(str string, tokens map[string]string) string {
	for k, v := range tokens {
		str = strings.ReplaceAll(str, k, v)
	}
	return str
}

func listDBAliasesFromConfig(args *Args) ([]string, error) {

	configPath, err := DiscoverConfigPath(args)
	if err != nil {
		return []string{}, err
	}

	readDatabaseConfig(configPath)

	dbAliases := make([]string, 0, len(DBConfig.Databases))
	for k := range DBConfig.Databases {
		dbAliases = append(dbAliases, k)
	}

	return dbAliases, nil
}
