#pragma once
#include <string>
void runMapper(std::string portno, int nPartition);
void doPartitionToChunks(int nPartition);
void doMapperProcess(std::string inputDirectory, std::string tempDirectory, std::string strPortno, std::string strThreads);


