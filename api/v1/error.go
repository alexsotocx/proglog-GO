package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrorOffsetOutOfrange struct {
	Offset uint64
}

func (e ErrorOffsetOutOfrange) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("offset out of range: %d", e.Offset))
	msg := fmt.Sprintf("The request offset is outside the log's range: %d", e.Offset)
	details := &errdetails.LocalizedMessage{
		Locale:  "en-Us",
		Message: msg,
	}
	std, err := st.WithDetails(details)
	if err != nil {
		return st
	}
	return std
}

func (e ErrorOffsetOutOfrange) Error() string {
	return e.GRPCStatus().Err().Error()
}
