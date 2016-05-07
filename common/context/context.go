package context

import (
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

func InitCtx(context *gin.Context) Ctx {
	requestID := context.Request.Header.Get("X-Request-Id")
	contextLogger := log.WithFields(log.Fields{
		"request-id": requestID,
	})
	return Ctx{Context: context, Log: contextLogger, RequestID: requestID}
}

type Ctx struct {
	Context   *gin.Context
	Log       *log.Entry
	RequestID string
}
