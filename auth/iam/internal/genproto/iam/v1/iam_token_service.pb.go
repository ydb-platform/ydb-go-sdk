// NOTE: this is statically generated file was put here manually.

package v1

import (
	context "context"
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type CreateIamTokenRequest struct {
	// Types that are valid to be assigned to Identity:
	//	*CreateIamTokenRequest_YandexPassportOauthToken
	//	*CreateIamTokenRequest_Jwt
	Identity             isCreateIamTokenRequest_Identity `protobuf_oneof:"identity"`
	XXX_NoUnkeyedLiteral struct{}                         `json:"-"`
	XXX_unrecognized     []byte                           `json:"-"`
	XXX_sizecache        int32                            `json:"-"`
}

func (m *CreateIamTokenRequest) Reset()         { *m = CreateIamTokenRequest{} }
func (m *CreateIamTokenRequest) String() string { return proto.CompactTextString(m) }
func (*CreateIamTokenRequest) ProtoMessage()    {}
func (*CreateIamTokenRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b373624a21e738a7, []int{0}
}

func (m *CreateIamTokenRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CreateIamTokenRequest.Unmarshal(m, b)
}
func (m *CreateIamTokenRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CreateIamTokenRequest.Marshal(b, m, deterministic)
}
func (m *CreateIamTokenRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CreateIamTokenRequest.Merge(m, src)
}
func (m *CreateIamTokenRequest) XXX_Size() int {
	return xxx_messageInfo_CreateIamTokenRequest.Size(m)
}
func (m *CreateIamTokenRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_CreateIamTokenRequest.DiscardUnknown(m)
}

var xxx_messageInfo_CreateIamTokenRequest proto.InternalMessageInfo

type isCreateIamTokenRequest_Identity interface {
	isCreateIamTokenRequest_Identity()
}

type CreateIamTokenRequest_YandexPassportOauthToken struct {
	YandexPassportOauthToken string `protobuf:"bytes,1,opt,name=yandex_passport_oauth_token,json=yandexPassportOauthToken,proto3,oneof"`
}

type CreateIamTokenRequest_Jwt struct {
	Jwt string `protobuf:"bytes,2,opt,name=jwt,proto3,oneof"`
}

func (*CreateIamTokenRequest_YandexPassportOauthToken) isCreateIamTokenRequest_Identity() {}

func (*CreateIamTokenRequest_Jwt) isCreateIamTokenRequest_Identity() {}

func (m *CreateIamTokenRequest) GetIdentity() isCreateIamTokenRequest_Identity {
	if m != nil {
		return m.Identity
	}
	return nil
}

func (m *CreateIamTokenRequest) GetYandexPassportOauthToken() string {
	if x, ok := m.GetIdentity().(*CreateIamTokenRequest_YandexPassportOauthToken); ok {
		return x.YandexPassportOauthToken
	}
	return ""
}

func (m *CreateIamTokenRequest) GetJwt() string {
	if x, ok := m.GetIdentity().(*CreateIamTokenRequest_Jwt); ok {
		return x.Jwt
	}
	return ""
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*CreateIamTokenRequest) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _CreateIamTokenRequest_OneofMarshaler, _CreateIamTokenRequest_OneofUnmarshaler, _CreateIamTokenRequest_OneofSizer, []interface{}{
		(*CreateIamTokenRequest_YandexPassportOauthToken)(nil),
		(*CreateIamTokenRequest_Jwt)(nil),
	}
}

func _CreateIamTokenRequest_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*CreateIamTokenRequest)
	// identity
	switch x := m.Identity.(type) {
	case *CreateIamTokenRequest_YandexPassportOauthToken:
		b.EncodeVarint(1<<3 | proto.WireBytes)
		b.EncodeStringBytes(x.YandexPassportOauthToken)
	case *CreateIamTokenRequest_Jwt:
		b.EncodeVarint(2<<3 | proto.WireBytes)
		b.EncodeStringBytes(x.Jwt)
	case nil:
	default:
		return fmt.Errorf("CreateIamTokenRequest.Identity has unexpected type %T", x)
	}
	return nil
}

func _CreateIamTokenRequest_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*CreateIamTokenRequest)
	switch tag {
	case 1: // identity.yandex_passport_oauth_token
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeStringBytes()
		m.Identity = &CreateIamTokenRequest_YandexPassportOauthToken{x}
		return true, err
	case 2: // identity.jwt
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeStringBytes()
		m.Identity = &CreateIamTokenRequest_Jwt{x}
		return true, err
	default:
		return false, nil
	}
}

func _CreateIamTokenRequest_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*CreateIamTokenRequest)
	// identity
	switch x := m.Identity.(type) {
	case *CreateIamTokenRequest_YandexPassportOauthToken:
		n += 1 // tag and wire
		n += proto.SizeVarint(uint64(len(x.YandexPassportOauthToken)))
		n += len(x.YandexPassportOauthToken)
	case *CreateIamTokenRequest_Jwt:
		n += 1 // tag and wire
		n += proto.SizeVarint(uint64(len(x.Jwt)))
		n += len(x.Jwt)
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type CreateIamTokenResponse struct {
	// IAM token for the specified identity.
	//
	// You should pass the token in the `Authorization` header for any further API requests.
	// For example, `Authorization: Bearer [iam_token]`.
	IamToken string `protobuf:"bytes,1,opt,name=iam_token,json=iamToken,proto3" json:"iam_token,omitempty"`
	// IAM token expiration time, in [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) text format.
	ExpiresAt            *timestamp.Timestamp `protobuf:"bytes,2,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *CreateIamTokenResponse) Reset()         { *m = CreateIamTokenResponse{} }
func (m *CreateIamTokenResponse) String() string { return proto.CompactTextString(m) }
func (*CreateIamTokenResponse) ProtoMessage()    {}
func (*CreateIamTokenResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_b373624a21e738a7, []int{1}
}

func (m *CreateIamTokenResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CreateIamTokenResponse.Unmarshal(m, b)
}
func (m *CreateIamTokenResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CreateIamTokenResponse.Marshal(b, m, deterministic)
}
func (m *CreateIamTokenResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CreateIamTokenResponse.Merge(m, src)
}
func (m *CreateIamTokenResponse) XXX_Size() int {
	return xxx_messageInfo_CreateIamTokenResponse.Size(m)
}
func (m *CreateIamTokenResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_CreateIamTokenResponse.DiscardUnknown(m)
}

var xxx_messageInfo_CreateIamTokenResponse proto.InternalMessageInfo

func (m *CreateIamTokenResponse) GetIamToken() string {
	if m != nil {
		return m.IamToken
	}
	return ""
}

func (m *CreateIamTokenResponse) GetExpiresAt() *timestamp.Timestamp {
	if m != nil {
		return m.ExpiresAt
	}
	return nil
}

func init() {
	proto.RegisterType((*CreateIamTokenRequest)(nil), "yandex.cloud.iam.v1.CreateIamTokenRequest")
	proto.RegisterType((*CreateIamTokenResponse)(nil), "yandex.cloud.iam.v1.CreateIamTokenResponse")
}

func init() {
	proto.RegisterFile("yandex/cloud/iam/v1/iam_token_service.proto", fileDescriptor_b373624a21e738a7)
}

var fileDescriptor_b373624a21e738a7 = []byte{
	// 384 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x92, 0xc1, 0x6e, 0xd3, 0x40,
	0x10, 0x86, 0x71, 0x40, 0x51, 0xb2, 0x48, 0x80, 0x36, 0x02, 0x05, 0x07, 0x04, 0xf2, 0x09, 0x25,
	0xca, 0x5a, 0x09, 0x27, 0xb8, 0x44, 0x09, 0x17, 0x38, 0x81, 0x4c, 0x4e, 0x5c, 0xac, 0xb5, 0x3d,
	0x84, 0x25, 0xb6, 0x77, 0xeb, 0x1d, 0xbb, 0x89, 0x54, 0xf5, 0xd0, 0x17, 0xe8, 0xa1, 0x2f, 0xd4,
	0x3e, 0x43, 0x5f, 0xa1, 0x0f, 0x52, 0xd9, 0xbb, 0xae, 0xd4, 0x2a, 0x87, 0x9e, 0x56, 0xab, 0xfd,
	0x76, 0xfe, 0x7f, 0xfe, 0x19, 0x32, 0xd9, 0xf3, 0x3c, 0x81, 0x9d, 0x1f, 0xa7, 0xb2, 0x4c, 0x7c,
	0xc1, 0x33, 0xbf, 0x9a, 0xd5, 0x47, 0x88, 0x72, 0x0b, 0x79, 0xa8, 0xa1, 0xa8, 0x44, 0x0c, 0x4c,
	0x15, 0x12, 0x25, 0x1d, 0x18, 0x98, 0x35, 0x30, 0x13, 0x3c, 0x63, 0xd5, 0xcc, 0x7d, 0xb7, 0x91,
	0x72, 0x93, 0x82, 0xcf, 0x95, 0xf0, 0x79, 0x9e, 0x4b, 0xe4, 0x28, 0x64, 0xae, 0xcd, 0x17, 0xf7,
	0x83, 0x7d, 0x6d, 0x6e, 0x51, 0xf9, 0xd7, 0x47, 0x91, 0x81, 0x46, 0x9e, 0x29, 0x0b, 0xbc, 0xbf,
	0x67, 0xa0, 0xe2, 0xa9, 0x48, 0x9a, 0x02, 0xe6, 0xd9, 0x3b, 0x25, 0xaf, 0xbf, 0x15, 0xc0, 0x11,
	0x7e, 0xf0, 0x6c, 0x5d, 0x5b, 0x0a, 0xe0, 0xa8, 0x04, 0x8d, 0x74, 0x41, 0x46, 0xe6, 0x67, 0xa8,
	0xb8, 0xd6, 0x4a, 0x16, 0x18, 0x4a, 0x5e, 0xe2, 0x3f, 0x63, 0x7c, 0xe8, 0x7c, 0x74, 0x3e, 0xf5,
	0xbf, 0x3f, 0x09, 0x86, 0x06, 0xfa, 0x65, 0x99, 0x9f, 0x35, 0xd2, 0xd4, 0xa1, 0x94, 0x3c, 0xfd,
	0x7f, 0x8c, 0xc3, 0x8e, 0x05, 0xeb, 0xcb, 0xea, 0x15, 0xe9, 0x89, 0x04, 0x72, 0x14, 0xb8, 0xa7,
	0xcf, 0x2e, 0xaf, 0x66, 0x8e, 0xa7, 0xc8, 0x9b, 0x87, 0xfa, 0x5a, 0xc9, 0x5c, 0x03, 0x1d, 0x91,
	0xfe, 0x5d, 0x4e, 0x46, 0x2e, 0xe8, 0x09, 0x0b, 0xd1, 0x2f, 0x84, 0xc0, 0x4e, 0x89, 0x02, 0x74,
	0xc8, 0x8d, 0xc6, 0xf3, 0xb9, 0xcb, 0x4c, 0x16, 0xac, 0xcd, 0x82, 0xad, 0xdb, 0x2c, 0x82, 0xbe,
	0xa5, 0x97, 0x38, 0x3f, 0x77, 0xc8, 0xcb, 0x56, 0xec, 0xb7, 0x89, 0x9f, 0x9e, 0x90, 0xae, 0x71,
	0x41, 0xc7, 0xec, 0xc0, 0x0c, 0xd8, 0xc1, 0x88, 0xdc, 0xc9, 0xa3, 0x58, 0xd3, 0x8e, 0xf7, 0xf6,
	0xec, 0xfa, 0xe6, 0xa2, 0x33, 0xf0, 0x5e, 0xb4, 0x4b, 0xd0, 0x34, 0xa6, 0xbf, 0x3a, 0xe3, 0xd5,
	0xf2, 0xcf, 0x82, 0xdb, 0x52, 0x53, 0x04, 0x9e, 0xb1, 0xa2, 0xb4, 0xe3, 0x8a, 0x04, 0x46, 0x65,
	0xbc, 0x05, 0xf4, 0x55, 0x19, 0xa5, 0x22, 0x9e, 0xd6, 0x4b, 0x70, 0x60, 0xa3, 0xa2, 0x6e, 0xd3,
	0xf3, 0xe7, 0xdb, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0x05, 0x0c, 0xf5, 0x6f, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// IamTokenServiceClient is the client API for IamTokenService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type IamTokenServiceClient interface {
	// Creates an IAM token for the specified identity.
	Create(ctx context.Context, in *CreateIamTokenRequest, opts ...grpc.CallOption) (*CreateIamTokenResponse, error)
}

type iamTokenServiceClient struct {
	cc *grpc.ClientConn
}

func NewIamTokenServiceClient(cc *grpc.ClientConn) IamTokenServiceClient {
	return &iamTokenServiceClient{cc}
}

func (c *iamTokenServiceClient) Create(ctx context.Context, in *CreateIamTokenRequest, opts ...grpc.CallOption) (*CreateIamTokenResponse, error) {
	out := new(CreateIamTokenResponse)
	err := c.cc.Invoke(ctx, "/yandex.cloud.iam.v1.IamTokenService/Create", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IamTokenServiceServer is the server API for IamTokenService service.
type IamTokenServiceServer interface {
	// Creates an IAM token for the specified identity.
	Create(context.Context, *CreateIamTokenRequest) (*CreateIamTokenResponse, error)
}

func RegisterIamTokenServiceServer(s *grpc.Server, srv IamTokenServiceServer) {
	s.RegisterService(&_IamTokenService_serviceDesc, srv)
}

func _IamTokenService_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateIamTokenRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IamTokenServiceServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/yandex.cloud.iam.v1.IamTokenService/Create",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IamTokenServiceServer).Create(ctx, req.(*CreateIamTokenRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _IamTokenService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "yandex.cloud.iam.v1.IamTokenService",
	HandlerType: (*IamTokenServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Create",
			Handler:    _IamTokenService_Create_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "yandex/cloud/iam/v1/iam_token_service.proto",
}
