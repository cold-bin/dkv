package idx

import (
	"log"
	
	"github.com/bwmarrin/snowflake"
)

func ID(node int64) int64 {
	n, err := snowflake.NewNode(node)
	if err != nil {
		log.Fatal(err)
	}
	
	return n.Generate().Int64()
}
