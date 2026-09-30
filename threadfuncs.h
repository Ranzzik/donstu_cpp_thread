#pragma once
#include <string>
#include <fstream>
#include <mutex>
#include <atomic>

inline constexpr int COUNT_THREADS = 4;
inline constexpr int COUNT_ITERATIONS = 10000; 

struct ThreadArgs {
    int id;
    std::string tag;
    std::string message;
};

class Logger {
public:
    explicit Logger(const std::string& filename);
    bool writeLine(const std::string& msg);
    int toggleMutex(bool enable);
// changed void in int
private:
    std::ofstream file_;
    std::mutex mutex_;
    bool useMutex_ = true;
};

extern int normalCounter;
extern std::atomic<int> atomicCounter;
