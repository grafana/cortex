package util

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// ipAddressesKey is key for the GRPC metadata where the IP addresses are stored
const ipAddressesKey = "x-forwarded-for"

// GetSourceFromOutgoingCtx extracts the source field from the GRPC context
func GetSourceFromOutgoingCtx(ctx context.Context) string {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return ""
	}
	ipAddresses, ok := md[ipAddressesKey]
	if !ok {
		return ""
	}
	return ipAddresses[0]
}

// GetSourceFromIncomingCtx extracts the source field from the GRPC context
func GetSourceFromIncomingCtx(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	ipAddresses, ok := md[ipAddressesKey]
	if !ok {
		return ""
	}
	return ipAddresses[0]
}

// AddSourceToOutgoingContext adds the given source to the GRPC context
func AddSourceToOutgoingContext(ctx context.Context, source string) context.Context {
	if source != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, ipAddressesKey, source)
	}
	return ctx
}

// AddSourceToIncomingContext adds the given source to the GRPC context
func AddSourceToIncomingContext(ctx context.Context, source string) context.Context {
	if source != "" {
		md := metadata.Pairs(ipAddressesKey, source)
		ctx = metadata.NewIncomingContext(ctx, md)
	}
	return ctx
}
