handle SIGUSR2 pass
handle SIGUSR2 nostop
handle SIGUSR2 noprint
b StrategyGetBufferLRU
r -D /home/hx/db/opengauss/datanode/test
