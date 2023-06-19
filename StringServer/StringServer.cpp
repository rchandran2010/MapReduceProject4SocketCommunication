/////////////////////////////////////////////////////////////////////////
// StringServer.cpp - Demonstrates simple one-way string messaging     //
// ver 1.1                                                             //
// Jim Fawcett, CSE687 - Object Oriented Design, Spring 2016           //
// Application: OOD Project #4                                         //
// Platform:    Visual Studio 2015, Dell XPS 8900, Windows 10 pro      //
/////////////////////////////////////////////////////////////////////////
/*
* This package was modified from StringServer.cpp written by Jim Fawcett.
*
* Required Files:
* ---------------
*   StringClient.cpp, StringServer.cpp
*   Sockets.h, Sockets.cpp
*   Logger.h, Logger.cpp, Cpp11-BlockingQueue.h
*   StringUtilities.h, StringUtilities.cpp
*
* Maintenance History:
* --------------------
* ver 1.1 : 19 Mar 2018f
* - updated to use Sockets, ver 5.3
* ver 1.0 : 04 Apr 2016
* - first release
*/
#include "../Sockets/Sockets.h"
#include "../Logger/Logger.h"
#include "../Utilities/StringUtilities/StringUtilities.h"
#include <string>
#include <iostream>
#include "../StringClient/CFileManager.h"
#include "MapperFunc.h"



using Show = StaticLogger<1>;
using namespace Sockets;
std::vector<std::string> configDirList;

//CFileManager FileManager;
int stub_portno;
int nPartition;
std::string commandtxt;


class ClientHandler
{
public:
  void operator()(Socket& socket_);
};

void ClientHandler::operator()(Socket& socket_)
{
  int stubCount = 1;
  while (true)
  {
    std::string msg = socket_.recvString();
    msg = Socket::removeTerminator(msg);
    Show::write("\n  server recvd message \"" + msg + "\"");
    if (msg == "quit") {
        // send message to controller all done. For now expect controller to send 2 Map
        stubCount++;
        if(stubCount > 1)
           break;
    }
    else
        configDirList.push_back(msg);
  }
  std::string serverCommand;
  if (commandtxt == "Mapper")
  {
      serverCommand = "MapperStub " + configDirList[0] + " " + configDirList[1] + " " + configDirList[2] + " " + std::to_string(stub_portno) + " " + std::to_string(nPartition);
      Show::write(serverCommand);
      system(serverCommand.c_str());
  }
  else if (commandtxt == "Reduce")
  {
      serverCommand = "ReducerStub " + configDirList[0] + " " + configDirList[1] + " " + configDirList[2] + " " + std::to_string(stub_portno) + " " + std::to_string(nPartition);
      Show::write(serverCommand);
      system(serverCommand.c_str());
  }
}

//----< test stub >--------------------------------------------------
int main(int argc, char* argv[])
{
  // To run server port_no partition(threads)
  Show::attach(&std::cout);
  Show::start();
  Show::title("\n  String Server started");

  //mappercall - Mapper 8080 2
  commandtxt  = argv[1];
  stub_portno = atoi(argv[2]);
  nPartition  = atoi(argv[3]);
 
  try
  {
    SocketSystem ss;
    SocketListener sl(stub_portno, Socket::IP6);
    ClientHandler cp;
    sl.start(cp);
    ::Sleep(100);

    Show::write("\n --------------------\n  press key to exit: \n --------------------");
    std::cout.flush();
    std::cin.get();
  }
  catch (std::exception& exc)
  {
    Show::write("\n  Exeception caught: ");
    std::string exMsg = "\n  " + std::string(exc.what()) + "\n\n";
    Show::write(exMsg);
  }
}