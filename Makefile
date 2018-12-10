test:
	gotest ./... -v

proto:
	protoc -I $$GOPATH/src \
	  -I=$$GOPATH/src/github.com/gogo/protobuf \
	  -I ./pandos pandos/pandos.proto \
	  --go_out=plugins=grpc:./pandos
