/////////////////////////////////////////////////////////////////////////
// StringClient.cpp - Demonstrates simple one-way string messaging     //
//                                                                     //
// Jim Fawcett, CSE687 - Object Oriented Design, Spring 2016           //
// Application: OOD Project #4                                         //
// Platform:    Visual Studio 2017, Dell XPS 8920, Windows 10 pro      //
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
 * ver 1.1 : 19 Mar 2018
 * - updated to use Sockets, ver 5.3
 * ver 1.0 : 04 Apr 2016
 * - first release
 */
#include "../Sockets/Sockets.h"
#include "../Logger/Logger.h"
#include "../Utilities/StringUtilities/StringUtilities.h"
#include "../Utilities/CodeUtilities/CodeUtilities.h"
#include <string>
#include <iostream>
#include <thread>
#include "CFileManager.h"

using Show = StaticLogger<1>;
using namespace Utilities;
using namespace Sockets;

int stub1_portno = 8080; // default.
int stub2_portno = 8080; // default. 


/////////////////////////////////////////////////////////////////////
// ClientCounter creates a sequential number for each client
//
class ClientCounter
{
public:
  ClientCounter() { ++clientCount; }
  size_t count() { return clientCount; }
private:
  static size_t clientCount;
};

size_t ClientCounter::clientCount = 0;

/////////////////////////////////////////////////////////////////////
// StringClient class
// - was created as a class so more than one instance could be 
//   run on child thread
//
class StringClient
{
public:
 // void execute(const size_t TimeBetweenMessages, const size_t NumMessages);
  void execute(const size_t portNo, const std::string stubFileConfig);
};

void StringClient::execute(const size_t portNo, const std::string stubFileConfig)
{
  ClientCounter counter;
  size_t myCount = counter.count();
  int stub1_portno = portNo; // default 
  std::string myCountString = Utilities::Converter<size_t>::toString(myCount);

  Show::attach(&std::cout);
  Show::start();

  Show::title(
    "Starting String client" + myCountString + 
    " on thread " + Utilities::Converter<std::thread::id>::toString(std::this_thread::get_id())
  );
  try
  {
    SocketSystem ss;
    SocketConnecter si;
//    while (!si.connect("localhost", 8080))
    while (!si.connect("localhost", stub1_portno))
    {
      Show::write("\n client waiting to connect");
      ::Sleep(100);
    }
    
    std::string msg;
    // read config file that has directories. 
    CFileManager FileManager;
    std::vector<std::string> linesInFile; 
    linesInFile = FileManager.readLinesInFile(stubFileConfig);

    for (size_t i = 0; i < linesInFile.size(); ++i)
    {
    //  msg = "message #" + Converter<size_t>::toString(i + 1) + " from client" + myCountString;
      msg = linesInFile[i];
      si.sendString(msg);
      Show::write("\n  client" + myCountString + " sent \"" + msg + "\"");
      ::Sleep(10);
    }
    msg = "quit";
    si.sendString(msg);
    Show::write("\n  client sent \"" + msg + "\"");

    Show::write("\n");
    Show::write("\n  All done folks");
  }
  catch (std::exception& exc)
  {
    Show::write("\n  Exeception caught: ");
    std::string exMsg = "\n  " + std::string(exc.what()) + "\n\n";
    Show::write(exMsg);
  }
}

int main(int argc, char* argv[])
{
  Show::title("Demonstrating two String Clients each running on a child thread");

  // Client runs with 2 port nos as input. 
  // Example StringClient 8099 8090

  int stub1_portno = atoi(argv[1]);
  int stub2_portno = atoi(argv[2]);

  std::string stubFileConfig1 = "C:\\ServerFiles\\ConfigFile1.txt";
  std::string stubFileConfig2 = "C:\\ServerFiles\\ConfigFile2.txt";

  StringClient c1;
  std::thread t1(
    [&]() { c1.execute(stub1_portno, stubFileConfig1); } // 50 messages 100 millisec apart
  );

  // Reducer will be running only in one port no. 
  // The server accepts two port no. If both are same its reducer. 
  if (stub1_portno != stub2_portno)
  {
      StringClient c2;
      std::thread t2(
          [&]() { c2.execute(stub2_portno, stubFileConfig2); } // 50 messages 120 millisec apart
      );
      t2.join();
  }
  t1.join();
   
}