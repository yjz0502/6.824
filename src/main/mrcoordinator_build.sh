go build -buildmode=plugin ../mrapps/jobcount.go
rm mr-out*
rm mr-*-*
rm WoerkerLogFile*.txt
rm ReduceLogFile*.txt
go run mrcoordinator.go pg-*.txt
