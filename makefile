CC = gcc # el compilador será gcc
CFLAGS = -Wall # opciones de compilación: -Wall para advertencias

all: p1-dataProgram p2-dataProgram

p2-dataProgram: p2-dataProgram.o
	$(CC) $(CFLAGS) p2-dataProgram.o -o p2-dataProgram

p1-dataProgram: p1-dataProgram.o
	$(CC) $(CFLAGS) p1-dataProgram.o -o p1-dataProgram

p2-dataProgram.o: p2-dataProgram.C
	$(CC) $(CFLAGS) -c p2-dataProgram.C

p1-dataProgram.o: p1-dataProgram.C
	$(CC) $(CFLAGS) -c p1-dataProgram.C

clean:
	rm -f *.o p2-dataProgram p1-dataProgram
