echo building... &
go build -buildmode=plugin ../mrapps/wc.go &
echo run master... &
go run mrmaster.go pg-*.txt &
sleep 1 &
echo run worker... &
go run mrworker.go wc.so
