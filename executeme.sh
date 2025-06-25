#!/bin/bash

# este script inicia el servidor en segundo plano y luego el cliente en primer plano
make;
./p2-dataProgram & ./p1-dataProgram;
make clean;
