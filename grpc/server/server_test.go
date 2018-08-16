package server

import (
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	msgmock "github.com/percona/mongodb-backup/mocks/mock_messages"
	pb "github.com/percona/mongodb-backup/proto/messages"
)

func TestServerAndClients(t *testing.T) {
	var err error
	type responseMsg struct {
		msg *pb.ClientMessage
		err error
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := msgmock.NewMockMessages_MessagesChatServer(ctrl)

	// We cannot use regular EXPECT()'s here since the client has Rec() & Send() in an infinite
	// for loop reading/writing from/to the stream.
	// To simulate a stream block until a new message arrives, I am using a channel were I
	// put the messages I want to send, when I want to send it so, the EXPECT will block until
	// there is a message in msgChan
	msgChan := make(chan responseMsg)
	clientID := "ABC123"

	stream.EXPECT().Recv().DoAndReturn(func() (*pb.ClientMessage, error) {
		for {
			response := <-msgChan
			return response.msg, response.err
		}
	}).AnyTimes()

	messagesServer := NewMessagesServer()
	// Start the chat server
	go func() {
		err = messagesServer.MessagesChat(stream) // this err var is global
	}()
	// Give some time so the go-routine can really start
	time.Sleep(150 * time.Millisecond)

	msgChan <- responseMsg{
		&pb.ClientMessage{
			Type:     pb.ClientMessage_REGISTER,
			ClientID: clientID,
		},
		nil,
	}
	time.Sleep(50 * time.Millisecond) // let the server process the message

	// Check if the client has been registered
	c := messagesServer.Clients()
	gotClient, ok := c[clientID]
	if !ok {
		t.Errorf("Registration failed. ClientID %s is not in clients list", clientID)
	}
	if gotClient.streaming != true {
		t.Errorf("Client is not streaming messages")
	}

	firstSeen := gotClient.LastSeen

	// Send and PONG and check the client has updated the LastSeen field
	msgChan <- responseMsg{
		&pb.ClientMessage{
			Type:     pb.ClientMessage_PONG,
			ClientID: clientID,
		},
		nil,
	}
	time.Sleep(50 * time.Millisecond) // let the server process the message

	if !gotClient.LastSeen.After(firstSeen) {
		t.Errorf("Pong didn't update last seen field. First seen: %v, last seen: %v", firstSeen, gotClient.LastSeen)
	}

	// Send EOF to stop the stream and unregister the client
	msgChan <- responseMsg{
		nil,
		io.EOF,
	}
	time.Sleep(50 * time.Millisecond) // let the server process the message

	c = messagesServer.Clients()
	if len(c) != 0 {
		t.Errorf("The client was not unregistered. Got: %+v", c)
	}
	if err != nil {
		t.Errorf("The server returned and error after EOF: %s", err)
	}
}
