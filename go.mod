module github.com/kagenihisomi/gogogo

// Add these lines to your root go.mod file
require github.com/kagenihisomi/datarizer v0.0.0-00010101000000-000000000000

replace github.com/kagenihisomi/datarizer => ./pkg

go 1.24.3

require (
	github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/mattn/go-sqlite3 v1.14.28
	github.com/xitongsys/parquet-go v1.6.2
)

require (
	github.com/aws/aws-sdk-go v1.55.7 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
)

require (
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40 // indirect
	github.com/apache/thrift v0.22.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/xitongsys/parquet-go-source v0.0.0-20241021075129-b732d2ac9c9b
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)
