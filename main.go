package main

import (
	"database/sql"
	"github.com/gin-gonic/gin"
	_ "github.com/heroku/x/hmetrics/onload"
	_ "github.com/lib/pq"
	"github.com/oschwald/geoip2-golang"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Announcement struct {
	NodeIP          string
	NodeID          string
	Tool            string
	SourceIP        string
	SourcePort      uint
	DestinationIP   string
	DestinationPort uint
	Type            string
	Timestamp       uint
}

func main() {
	port := os.Getenv("PORT")

	if port == "" {
		log.Fatal("$PORT must be set")
	}

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/static", "static")

	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl.html", nil)
	})

	db, _ := sql.Open("postgres", os.Getenv("DATABASE_URL"))

	router.POST("/ingest/packet", func(c *gin.Context) {
		var announcement Announcement

		// Check the client sent valid JSON
		if err := c.BindJSON(&announcement); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"Status": err.Error()})
			return
		}

		// Check all fields are there
		if announcement.NodeIP == "" {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "NodeIP not provided"})
			return
		} else if announcement.NodeID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "NodeID not provided"})
			return
		} else if announcement.SourceIP == "" {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "SourceIP not provided"})
			return
		} else if announcement.SourcePort == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "SourcePort not provided"})
			return
		} else if announcement.DestinationIP == "" {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "DestinationIP not provided"})
			return
		} else if announcement.DestinationPort == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "DestinationPort not provided"})
			return
		} else if announcement.Type == "" {
			c.JSON(http.StatusBadRequest, gin.H{"Status": "Type not provided"})
			return
		}

		// Save packet and send OK message
		saveErr := savePacket(announcement, db)
		if saveErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"Status": saveErr.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"Status": "OK"})
	})

	router.Run(":" + port)
}

func savePacket(json Announcement, db *sql.DB) error {

	if json.Tool == "libprotoident" {
		json.Tool = "0"
	} else {
		json.Tool = "1"
	}

	if json.Timestamp == 0 {
		json.Timestamp = uint(time.Now().Unix())
	}

	geo, err := geoip2.Open("GeoLite2-City.mmdb")
	if err != nil {
		log.Fatal(err)
	}
	defer geo.Close()
	// If you are using strings that may be invalid, check that ip is not nil
	srcCountry := "XX"
	dstCountry := "XX"
	srcLat := "0.0"
	srcLng := "0.0"
	dstLat := "0.0"
	dstLng := "0.0"
	srcIP := net.ParseIP(json.SourceIP)
	dstIP := net.ParseIP(json.DestinationIP)
	srcRecord, srcErr := geo.City(srcIP)
	if srcErr == nil {
		srcCountry = srcRecord.Country.IsoCode
		srcLat = FloatToString(srcRecord.Location.Latitude)
		srcLng = FloatToString(srcRecord.Location.Longitude)
	}
	dstRecord, dstErr := geo.City(dstIP)
	if dstErr == nil {
		dstCountry = dstRecord.Country.IsoCode
		dstLat = FloatToString(dstRecord.Location.Latitude)
		dstLng = FloatToString(dstRecord.Location.Longitude)
	}
	var lastInsertId int
	insertErr := db.QueryRow("INSERT INTO \"public\".\"packets\" (\"node_id\", \"node_ip\", \"destination_ip\", \"destination_port\", \"tool\", \"source_ip\", \"source_port\", \"packet_type\", \"id\", \"source_country\", \"destination_country\", \"source_latitude\", \"source_longitude\", \"destination_latitude\", \"destination_longitude\", \"timestamp\") VALUES ('" + json.NodeID + "', '" + json.NodeIP + "', '" + json.DestinationIP + "', " + strconv.Itoa(int(json.DestinationPort)) + ", '" + json.Tool + "', '" + json.SourceIP + "', " + strconv.Itoa(int(json.SourcePort)) + ", '" + json.Type + "', DEFAULT, '" + srcCountry + "', '" + dstCountry + "', " + srcLat + ", '" + srcLng + "', '" + dstLat + "', '" + dstLng + "', " + strconv.Itoa(int(json.Timestamp)) + ") returning id;").Scan(&lastInsertId)
	if insertErr != nil {
		return insertErr
	}
	log.Println("last inserted id =", lastInsertId)
	return nil
}

func FloatToString(input_num float64) string {
	// to convert a float number to a string
	return strconv.FormatFloat(input_num, 'f', 6, 64)
}
