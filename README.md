aws Glue/Spark study
===
This repo have intent to aid run small snipets of aws glue localy in a containered

to init env
```shell
$ make up-container
```

to enter in environment
```shell
$ make attach
```

and play



# Stream example

up server in port 9999
```
apt install netcat -y
nc -lk 9999 // and tipe
or
watch echo "hello world" | nc -l -p 9999
```

after in another terminal
```
spark-submit /home/project/stream/spark/network_wordcount.py localhost 9999
```

```
/home/project# nc -l -p 9999
hello world
hello world
hello
world
hello hello
hell
word
wello hord
hello world
bla bla bla
bli bli bli
bib bi bi
bla bla bla
```