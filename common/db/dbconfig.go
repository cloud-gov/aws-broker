package db

import (
	// This is to init the mysql driver
	_ "github.com/go-sql-driver/mysql"
	// This is to init the postgres driver
	_ "github.com/lib/pq"
	// This is to init the sqlite driver
	_ "github.com/mattn/go-sqlite3"

	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"log"
)

// Config holds configuration information to connect to a database.
// Parameters for the config.
// * dbname - The name of the database to connect to
// * user - The user to sign in as
// * password - The user's password
// * host - The host to connect to. Values that start with / are for unix domain sockets.
//   (default is localhost)
// * port - The port to bind to. (default is 5432)
// * sslmode - Whether or not to use SSL (default is require, this is not the default for libpq)
//   Valid SSL modes:
//    * disable - No SSL
//    * require - Always SSL (skip verification)
//    * verify-full - Always SSL (require verification)
type Config struct {
	DbType   string `yaml:"db_type" validate:"required"`
	URL      string `yaml:"url" validate:"required"`
	Username string `yaml:"username" validate:"required"`
	Password string `yaml:"password" validate:"required"`
	DbName   string `yaml:"db_name" validate:"required"`
	Sslmode  string `yaml:"ssl_mode" validate:"required"`
	Port     int64  `yaml:"port" validate:"required"` // Is int64 to match the type that rds.Endpoint.Port is in the AWS RDS SDK.
}

// Init is a generic helper function that will try to connect to a database with the config in the input.
// Supported DB types:
// * postgres
// * mysql
// * sqlite3
func Init(dbConfig *Config) (*gorm.DB, error) {
	var DB gorm.DB
	var err error
	/*
		log.Printf("Attempting to login as %s with password length %d and url %s to db name %s\n",
			dbConfig.Username,
			len(dbConfig.Password),
			dbConfig.URL,
			dbConfig.DbName)
	*/
	switch dbConfig.DbType {
	case "postgres":
		conn := "dbname=%s user=%s password=%s host=%s sslmode=%s port=%d"
		conn = fmt.Sprintf(conn,
			dbConfig.DbName,
			dbConfig.Username,
			dbConfig.Password,
			dbConfig.URL,
			dbConfig.Sslmode,
			dbConfig.Port)
		DB, err = gorm.Open(dbConfig.DbType, conn)
	case "mysql":
		/*
			sslmode := "skip-verify"
			if dbConfig.Sslmode == "true" {
				sslmode = "tls-on"
				certFile := filepath.Join("resources", "rds-ca-2015-root.pem")
				certData, err := ioutil.ReadFile(certFile)
				if err != nil {
					log.Fatalf("error: %v", err)
				}
				block, _ := pem.Decode(certData)
				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					log.Fatalf("error: %v", err)
				}
				roots := x509.NewCertPool()
				roots.AddCert(cert)
				mysql.RegisterTLSConfig(sslmode, &tls.Config{ServerName: dbConfig.URL, RootCAs: roots})
			}
			//conn := "%s:%s@%s(%s:%d)/%s?charset=utf8&parseTime=True"
			conn := "%s:%s@%s(%s:%d)/%s?tls=%s&charset=utf8&parseTime=True"
			conn = fmt.Sprintf(conn,
				dbConfig.Username,
				dbConfig.Password,
				"tcp",
				dbConfig.URL,
				dbConfig.Port,
				dbConfig.DbName,
				sslmode)
		*/
		conn := "%s:%s@%s(%s:%d)/%s?charset=utf8&parseTime=True"
		conn = fmt.Sprintf(conn,
			dbConfig.Username,
			dbConfig.Password,
			"tcp",
			dbConfig.URL,
			dbConfig.Port,
			dbConfig.DbName)
		DB, err = gorm.Open(dbConfig.DbType, conn)
	case "sqlite3":
		DB, err = gorm.Open("sqlite3", dbConfig.DbName)
	default:
		errorString := "Cannot connect. Unsupported DB type: (" + dbConfig.DbType + ")"
		log.Println(errorString)
		return nil, errors.New(errorString)
	}
	if err != nil {
		log.Println("Error!" + err.Error())
		return nil, err
	}

	if err = DB.DB().Ping(); err != nil {
		log.Println("Unable to verify connection to database")
		return nil, err
	}
	return &DB, nil
}
