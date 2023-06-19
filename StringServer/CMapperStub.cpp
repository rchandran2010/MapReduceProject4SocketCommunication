#include "CMapperStub.h"
void CMapperStub::runMapper(std::string portno, int nPartition)
{
    // runMapper 
    std::multimap<int, std::string>::iterator itr;
    std::string strline;
    std::string strFileName;
    std::string strPartition;
    std::string tempfilename = "PART0000_" + portno + "_";

    int static cond = 0;
    static int x = nPartition;
    //wi = th;
    std::unique_lock<std::mutex> lk(cv_m);
    cv.wait(lk, [nPartition] {return cond != nPartition; });

    /* Mapper uses Map function from CMapperLibrary DLL.  */
    HINSTANCE hDLLMapper;
    const wchar_t* libNameMapper = L"CMapperLibrary";

    hDLLMapper = LoadLibraryEx(libNameMapper, NULL, NULL); // Handle to DLL
    if (hDLLMapper != NULL) {
        CREATE_MAPPER_OBJ  MapperPtr = (CREATE_MAPPER_OBJ)GetProcAddress(hDLLMapper, "CreateMapperObject");
        ICMapper* pMapper = MapperPtr();

        if (pMapper != NULL) {
            for (itr = filesInBatch.begin(); (itr != filesInBatch.end()); ++itr)
            {
                //if (itr->first != R) continue;
                if (itr->first != nPartition) continue;

                std::vector<std::string> linesInFile1;
                strPartition = std::to_string(itr->first);

                strFileName = itr->second;
                std::cout << std::endl;
                std::cout << std::endl << "Running Thread - " << strPartition << " Processing File - " << strFileName << std::endl;
                linesInFile1.clear();
                linesInFile1 = FileManager.readLinesInFile(strFileName);

                for (std::size_t j = 0; j < linesInFile1.size(); ++j)
                {
                    std::vector<std::string> words;

                    // Call to mapper to tokenize line.
                    char space_char = ' ';
                    std::string strTokenWord = linesInFile1[j];
                    std::string strTokenWord1 = pMapper->Map(strFileName, strTokenWord);

                    // Call Mapper DLL to get tokenized words. 
                    boost::split(words, pMapper->Map(strFileName, strTokenWord), boost::is_any_of(" "));

                    /* Map outputs a separate temporary file that holds (word, 1) for each occurrence of every word.  */
                    std::string bucket = std::to_string(nPartition);
                    std::string tmpfile;
                    tmpfile = tempfilename + bucket;
                    /*
                      The export function will buffer output in memory and periodically write the data out to disk
                    */
                    if (words.size() > 0)
                        FileManager.writeTempOutputFile(tmpfile, words);
                    words.clear();
                }
            }
        }
        else
            std::cout << "Did not load mapper library correctly." << std::endl;
        FreeLibrary(hDLLMapper);
    }
    else {
        std::cout << "Mapper Library load failed!" << std::endl;
    }
    cond = nPartition;
    cv.notify_one();
};

/* This function will partion the files in input directory into chunks based on thread count.*/
void CMapperStub::doPartitionToChunks(int nPartition) {

    FileManager.readDirectory(FileManager.getInputFileDirectory());
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
};

void CMapperStub::doMapperProcess(std::string inputDirectory, std::string tempDirectory, std::string strPortno, std::string strThreads)
{
    int R = 0;
    /*
     1)	Your program will accept three inputs via command-line:
     a)	Directory that holds input files.
     b)	Directory to hold output files.
     c)	Temporary directory to hold intermediate output files.
    */

    /* verify input directory.*/
    if (FileManager.isValidDirectory(inputDirectory))
        FileManager.setInputFileDirectory(inputDirectory);
    else
    {
        std::cout << "Please enter a valid Input directory" << std::endl;
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
    // Set portno. 
    std::string portno = strPortno; // Initialize thread count. 

    // Set partition value. 
    R = stoi(strThreads); // Initialize thread count. 

    // clean temp directory.
    std::string cleanTempDirCmd = "del " + FileManager.getTempFileDirectory() + "\\PART0000*.txt";
    std::cout << cleanTempDirCmd << std::endl;
    system(cleanTempDirCmd.c_str());

    /*******************************  MAP  ***********************************/
    /*
    a)	Map: Is given data from a file (does not parse the file itself)
    */
    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << "Mapper is now running....." << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << std::endl;
    std::cout << "Starting Mapper With ...." << std::endl;
    std::cout << "Input Directory > " << inputDirectory << std::endl;
    std::cout << "Temp Directory  > " << tempDirectory << std::endl;
    std::cout << "From Port No    > " << strPortno << std::endl;
    std::cout << "Thread count    > " << strThreads << std::endl;
    std::cout << std::endl;

    doPartitionToChunks(R);

    // call Mapper using threads for R partition.
    std::vector<std::thread> map_threads;
    for (int i = 1; i <= R; i++) {
        // add partition i to filename. 
        map_threads.emplace_back(std::thread(CMapperStub::runMapper, portno, i));
    }
    for (auto& t : map_threads)
        t.join();
    return;
};
