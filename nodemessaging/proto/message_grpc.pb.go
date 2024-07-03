// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.25.1
// source: nodemessaging/proto/message.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// MessageCenterClient is the client API for MessageCenter service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MessageCenterClient interface {
	// The clients call this method to build a event channel from server to client.
	SubscribeEvents(ctx context.Context, in *CallerInfo, opts ...grpc.CallOption) (MessageCenter_SubscribeEventsClient, error)
	// The clients call this method to build a command channel from client to server.
	SendCommands(ctx context.Context, opts ...grpc.CallOption) (MessageCenter_SendCommandsClient, error)
}

type messageCenterClient struct {
	cc grpc.ClientConnInterface
}

func NewMessageCenterClient(cc grpc.ClientConnInterface) MessageCenterClient {
	return &messageCenterClient{cc}
}

func (c *messageCenterClient) SubscribeEvents(ctx context.Context, in *CallerInfo, opts ...grpc.CallOption) (MessageCenter_SubscribeEventsClient, error) {
	stream, err := c.cc.NewStream(ctx, &MessageCenter_ServiceDesc.Streams[0], "/proto.MessageCenter/subscribeEvents", opts...)
	if err != nil {
		return nil, err
	}
	x := &messageCenterSubscribeEventsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type MessageCenter_SubscribeEventsClient interface {
	Recv() (*EventMessage, error)
	grpc.ClientStream
}

type messageCenterSubscribeEventsClient struct {
	grpc.ClientStream
}

func (x *messageCenterSubscribeEventsClient) Recv() (*EventMessage, error) {
	m := new(EventMessage)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *messageCenterClient) SendCommands(ctx context.Context, opts ...grpc.CallOption) (MessageCenter_SendCommandsClient, error) {
	stream, err := c.cc.NewStream(ctx, &MessageCenter_ServiceDesc.Streams[1], "/proto.MessageCenter/sendCommands", opts...)
	if err != nil {
		return nil, err
	}
	x := &messageCenterSendCommandsClient{stream}
	return x, nil
}

type MessageCenter_SendCommandsClient interface {
	Send(*CommandMessage) error
	CloseAndRecv() (*emptypb.Empty, error)
	grpc.ClientStream
}

type messageCenterSendCommandsClient struct {
	grpc.ClientStream
}

func (x *messageCenterSendCommandsClient) Send(m *CommandMessage) error {
	return x.ClientStream.SendMsg(m)
}

func (x *messageCenterSendCommandsClient) CloseAndRecv() (*emptypb.Empty, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(emptypb.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// MessageCenterServer is the server API for MessageCenter service.
// All implementations must embed UnimplementedMessageCenterServer
// for forward compatibility
type MessageCenterServer interface {
	// The clients call this method to build a event channel from server to client.
	SubscribeEvents(*CallerInfo, MessageCenter_SubscribeEventsServer) error
	// The clients call this method to build a command channel from client to server.
	SendCommands(MessageCenter_SendCommandsServer) error
	mustEmbedUnimplementedMessageCenterServer()
}

// UnimplementedMessageCenterServer must be embedded to have forward compatible implementations.
type UnimplementedMessageCenterServer struct {
}

func (UnimplementedMessageCenterServer) SubscribeEvents(*CallerInfo, MessageCenter_SubscribeEventsServer) error {
	return status.Errorf(codes.Unimplemented, "method SubscribeEvents not implemented")
}
func (UnimplementedMessageCenterServer) SendCommands(MessageCenter_SendCommandsServer) error {
	return status.Errorf(codes.Unimplemented, "method SendCommands not implemented")
}
func (UnimplementedMessageCenterServer) mustEmbedUnimplementedMessageCenterServer() {}

// UnsafeMessageCenterServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MessageCenterServer will
// result in compilation errors.
type UnsafeMessageCenterServer interface {
	mustEmbedUnimplementedMessageCenterServer()
}

func RegisterMessageCenterServer(s grpc.ServiceRegistrar, srv MessageCenterServer) {
	s.RegisterService(&MessageCenter_ServiceDesc, srv)
}

func _MessageCenter_SubscribeEvents_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(CallerInfo)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MessageCenterServer).SubscribeEvents(m, &messageCenterSubscribeEventsServer{stream})
}

type MessageCenter_SubscribeEventsServer interface {
	Send(*EventMessage) error
	grpc.ServerStream
}

type messageCenterSubscribeEventsServer struct {
	grpc.ServerStream
}

func (x *messageCenterSubscribeEventsServer) Send(m *EventMessage) error {
	return x.ServerStream.SendMsg(m)
}

func _MessageCenter_SendCommands_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(MessageCenterServer).SendCommands(&messageCenterSendCommandsServer{stream})
}

type MessageCenter_SendCommandsServer interface {
	SendAndClose(*emptypb.Empty) error
	Recv() (*CommandMessage, error)
	grpc.ServerStream
}

type messageCenterSendCommandsServer struct {
	grpc.ServerStream
}

func (x *messageCenterSendCommandsServer) SendAndClose(m *emptypb.Empty) error {
	return x.ServerStream.SendMsg(m)
}

func (x *messageCenterSendCommandsServer) Recv() (*CommandMessage, error) {
	m := new(CommandMessage)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// MessageCenter_ServiceDesc is the grpc.ServiceDesc for MessageCenter service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MessageCenter_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.MessageCenter",
	HandlerType: (*MessageCenterServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "subscribeEvents",
			Handler:       _MessageCenter_SubscribeEvents_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "sendCommands",
			Handler:       _MessageCenter_SendCommands_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "nodemessaging/proto/message.proto",
}