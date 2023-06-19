// ReducerStub.cpp : This file contains the 'main' function. Program execution begins and ends there.
//
#include <iostream>
#include "CFileManager.h"
#include <windows.h>
#include "ReducerInterface.h"
#include <thread>
#include <mutex>
#include <boost/algorithm/string.hpp>

std::multimap<int, std::string> filesInBatch;
//std::multimap<int, std::string> MergedlinesInFile;

std::map<std::string, std::string> wordCountMap;

std::vector<std::thread> map_threads;
CFileManager FileManager;
typedef ICReducer* (*CREATE_REDUCER_OBJ)();

std::condition_variable cv_reducer;
std::mutex cv_r; // This mutex is used to synchronize accesses for reducer processing

void do_accumulate_job(int nPartition)
{
    // runMapper 
    std::multimap<int, std::string>::iterator itr;
    std::string strline;
    std::string strFileName;
    std::string strPartition;
    std::string tempfilename = "PART0000_";

    int static rcond = 1;
    static int rx = nPartition;
    std::unique_lock<std::mutex> lk(cv_r);
    cv_reducer.wait(lk, [nPartition] {return rcond != nPartition; });

    for (itr = filesInBatch.begin(); (itr != filesInBatch.end()); ++itr)
    {
         //if (itr->first != R) continue;
         if (itr->first != nPartition) continue;

         std::vector<std::string> linesInFile;
         strPartition = std::to_string(itr->first);

         strFileName = itr->second;
         std::cout << std::endl;
         std::cout << std::endl << "Running Thread - " << strPartition << " Processing File - " << strFileName << std::endl;
         linesInFile.clear();
         linesInFile = FileManager.readLinesInFile(strFileName);

         for (std::size_t j = 0; j < linesInFile.size(); ++j)
         {
             const std::string lower_sline = boost::algorithm::to_lower_copy(linesInFile[j]);
             std::vector<std::string> words;
             // split into words array with delimiter '-'. So ACT - 1 would be stored as word[0]=ACT and word[1]=1 
             boost::split(words, lower_sline, boost::is_any_of("-"));
             // check if words exist add 1 to accumualte else add word to list.
             if (wordCountMap.find(words[0]) == wordCountMap.end())
                 // add word and count.
                 wordCountMap.insert(std::pair<std::string, std::string>(words[0], words[1]));
             else
                 wordCountMap[words[0]] = wordCountMap[words[0]] + "1";
         }
    }
    rcond = nPartition;
    cv_reducer.notify_one();
};

/* This function will partion the files in input directory into chunks based on thread count.*/
void doPartitionToChunks(int nPartition) {

    FileManager.readDirectory(FileManager.getTempFileDirectory());
    // read files from input directory.
    std::vector<std::string> listOfFiles = FileManager.getFilesInDirectory();

    // Caluculate the chunk size each partition should store.  
    int totFiles = listOfFiles.size();
    int chunks = abs(totFiles / nPartition);

    // Partition files to process based on the partition key using R. 
    for (int i = 0, ctr = 0; (ctr < totFiles && i < nPartition); i++) {
        for (int j = 0; j < chunks; j++) {
            if (ctr >= totFiles) break;
            filesInBatch.insert(std::pair<int, std::string>(i + 1, listOfFiles.at(ctr++)));
        }
    }
    int batchsize = filesInBatch.size();
    if (batchsize < totFiles)
    {
        int i = 1; // index starts at 1.
        while (batchsize < totFiles) {
            filesInBatch.insert(std::pair<int, std::string>(i++, listOfFiles.at(batchsize++)));
        }
    }
}

void resetAccumulator() {
    wordCountMap.clear();
}

int runReducer() {
    HINSTANCE hDLLReducer;
    const wchar_t* libNameReducer = L"CReducerLibrary";
    hDLLReducer = LoadLibraryEx(libNameReducer, NULL, NULL); // Handle to DLL
    if (hDLLReducer != NULL)
    {
        CREATE_REDUCER_OBJ  ReducerPtr = (CREATE_REDUCER_OBJ)GetProcAddress(hDLLReducer, "CreateReducerObject");
        ICReducer* pReducer = ReducerPtr();

        if (pReducer != NULL)
        {
            pReducer->Reduce(wordCountMap);
            std::string fname = pReducer->getReducerFileName();
            FileManager.writeOutputFile(pReducer->getReducerFileName(), pReducer->exportData());
            // clear buffer 
            pReducer->bufferFlush();
            FreeLibrary(hDLLReducer);
        }
        else
            std::cout << "Did not load reducer Library correctly." << std::endl;
        return 1;
    }
    else {
        std::cout << "Reducer Library load failed!" << std::endl;
        return 2;
    }
}

void doReducerProcess(std::string outputDirectory, std::string tempDirectory,std::string strThreads) 
 {
 
    /* verify output directory.*/
    if (FileManager.isValidDirectory(outputDirectory))
        FileManager.setOuputFileDirectory(outputDirectory);
    else
    {
        std::cout << "Please enter a valid Output directory" << std::endl;
        return;
    }

    /* verify temp directory.*/
    if (FileManager.isValidDirectory(tempDirectory))
        FileManager.setTempFileDirectory(tempDirectory);
    else
    {
        std::cout << "Please enter a valid Temp directory" << std::endl;
        return;
    }
    
    int R = stoi(strThreads); // Initialize thread count. 

    // clean output directory.
    std::string cleanOutputDirCmd = "del " + FileManager.getOuputFileDirectory() + "\\reduce*.txt";
    std::cout << cleanOutputDirCmd << std::endl;
    system(cleanOutputDirCmd.c_str());

    cleanOutputDirCmd = "del " + FileManager.getOuputFileDirectory() + "\\SUCCESS.txt";
    std::cout << cleanOutputDirCmd << std::endl;
    system(cleanOutputDirCmd.c_str());

    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << "Reducer Process in progress....." << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << std::endl;
    std::cout << "Starting Reducer  ...." << std::endl;
    std::cout << "Ouput Directory > " << outputDirectory << std::endl;
    std::cout << "Temp Directory  > " << tempDirectory << std::endl;
    std::cout << "Thread count    > " << strThreads << std::endl;
    std::cout << std::endl;

    // parition files in temp directory to equal chunks to be processed by reducer threads. 
    doPartitionToChunks(R);
    resetAccumulator();
    std::cout << "Sorting and Accumulation in process" << std::endl;

    // call Mapper using threads for R partition.
    std::vector<std::thread> reducer_threads;
    for (int i = 1; i <= R; i++) {
        reducer_threads.emplace_back(std::thread(do_accumulate_job, i));
    }
    for (auto& t : reducer_threads)
        t.join();
    std::cout << "Sorting and Accumulation complete" << std::endl;
    // run reducer to produce reducer output file.
    std::cout << "Reducer DLL process is now running " << std::endl;
    runReducer();
    // Write Success File.
    FileManager.writeEmptySuccessfile();
    std::cout << "Map Reduce Process finished. " << std::endl;
    std::cout << "Hello World!\n";
}

int main(int argc, char* argv[])
{
    std::cout << "appname " << argv[0] << std::endl;
    int R = 0;
    /*
     1)	Your program will accept three inputs via command-line:
     a)	Directory that holds input files.
     b)	Directory to hold output files.
     c)	Temporary directory to hold intermediate output files.
    */
    if (argc != 6)
    {
        std::cout << "Usage: MapperStub <DIRECTORY FOR INPUT FILE> <DIRECTORY FOR OUTPUT FILE> <DIRECTORY FOR TEMP FILE> <THREAD COUNT> " << std::endl;
        return 1;
    }

    /* verify input directory.*/
    if (FileManager.isValidDirectory(argv[1]))
        FileManager.setInputFileDirectory(argv[1]);
    else
    {
        std::cout << "Please enter a valid Input directory" << std::endl;
        return 1;
    }

    /* verify output directory.*/
    if (FileManager.isValidDirectory(argv[2]))
        FileManager.setOuputFileDirectory(argv[2]);
    else
    {
        std::cout << "Please enter a valid Output directory" << std::endl;
        return 1;
    }

    /* verify temp directory.*/
    if (FileManager.isValidDirectory(argv[3]))
        FileManager.setTempFileDirectory(argv[3]);
    else
    {
        std::cout << "Please enter a valid Temp directory" << std::endl;
        return 1;
    }
    // Set portno. 
    std::string portno = argv[4]; // Initialize thread count. 

    // Set partition value. 
    R = atoi(argv[5]); // Initialize thread count. 

    doReducerProcess(argv[2], argv[3], argv[5]);
    return 0;
    
    /*
    // clean output directory.
    std::string cleanOutputDirCmd = "del " + FileManager.getOuputFileDirectory() + "\\reduce*.txt";
    std::cout << cleanOutputDirCmd << std::endl;
    system(cleanOutputDirCmd.c_str());

    cleanOutputDirCmd = "del " + FileManager.getOuputFileDirectory() + "\\SUCCESS.txt";
    std::cout << cleanOutputDirCmd << std::endl;
    system(cleanOutputDirCmd.c_str());

    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << "Reducer is now running....." << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << std::endl;
    std::cout << "Starting Reducer With ...." << std::endl;
    std::cout << "Input Directory > " << argv[1] << std::endl;
    std::cout << "Ouput Directory > " << argv[2] << std::endl;
    std::cout << "Temp Directory  > " << argv[3] << std::endl;
    std::cout << "From Port No    > " << argv[4] << std::endl;
    std::cout << "Thread count    > " << argv[5] << std::endl;
    std::cout << std::endl;

    // parition files in temp directory to equal chunks to be processed by reducer threads. 
    doPartitionToChunks(R);
    resetAccumulator();
    std::cout << "Sorting and Accumulation in process" << std::endl;

    // call Mapper using threads for R partition.
    std::vector<std::thread> reducer_threads;
    for (int i = 1; i <= R; i++) {
        reducer_threads.emplace_back(std::thread(do_accumulate_job,i));
    }
    for (auto& t : reducer_threads)
        t.join();
    std::cout << "Sorting and Accumulation complete" << std::endl;
    // run reducer to produce reducer output file.
    std::cout << "Reducer DLL process is now running " << std::endl;
    runReducer();
    // Write Success File.
    FileManager.writeEmptySuccessfile();
    std::cout << "Map Reduce Process finished. " << std::endl;
    std::cout << "Hello World!\n";
    */
 }
