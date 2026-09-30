#include <iostream>
#include <vector>
#include <thread>
#include <sstream>
#include <future>
#include <chrono>
#include <unistd.h>
#include <sys/syscall.h>
#include "threadfuncs.h"

Logger logger("output.log");

void counterWorker() {
    for (int i = 0; i < 100000; ++i) {
        normalCounter++;
        atomicCounter++;
    }
}

std::string valueWorker(ThreadArgs& args) {
    int counter = 0;
    for (int i = 0; i < COUNT_ITERATIONS; ++i) {
        counter++;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return "РџРѕС‚РѕРє " + args.tag + " Р·Р°РІРµСЂС€РёР» " + std::to_string(counter) + " С€Р°РіРѕРІ.";
}

std::mutex cpMutex;
std::condition_variable cv;
int buffer = -1;
bool ready = false;
bool done = false;

void producer() {
    for (int i = 1; i <= 10; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
        {
            std::lock_guard<std::mutex> lock(cpMutex);
            buffer = i;
            ready = true;
            logger.writeLine("[Producer] Р—Р°РїРёСЃР°Р» РІ Р±СѓС„РµСЂ: " + std::to_string(i));
        }
        cv.notify_one(); 
    }
    {
        std::lock_guard<std::mutex> lock(cpMutex);
        done = true;
    }
    cv.notify_one();
}

void consumer() {
    while (true) {
        std::unique_lock<std::mutex> lock(cpMutex);
        cv.wait(lock, [] { return ready || done; });

        if (ready) {
            logger.writeLine("[Consumer] РџСЂРѕС‡РёС‚Р°Р» РёР· Р±СѓС„РµСЂР°: " + std::to_string(buffer));
            ready = false;
        }
        if (done && !ready) {
            logger.writeLine("[Consumer] РљРѕРЅРµС† СЂР°Р±РѕС‚С‹. Р’С‹С…РѕРґ.");
            break;
        }
    }
}

int main() {
    logger.writeLine("main: СЃС‚Р°СЂС‚. PID: " + std::to_string(getpid()));

    std::vector<std::thread> threads;
    std::vector<ThreadArgs> args(COUNT_THREADS);

    for (int i = 0; i < COUNT_THREADS; ++i) {
        std::ostringstream oss;
        oss << "T" << i;
        args[i].id = i;
        args[i].tag = oss.str();
    }

    for (int i = 0; i < COUNT_THREADS; ++i) {
        threads.emplace_back(std::thread([i, &args]() {
            for (int j = 0; j < COUNT_ITERATIONS; ++j) {
                std::ostringstream ss;
                ss << "[РќРёС‚СЊ: " << args[i].tag 
                   << "] РЁР°Рі: " << j
                   << " | Kernel TID: " << syscall(SYS_gettid)
                   << " | std::thread::id: " << std::this_thread::get_id();
                logger.writeLine(ss.str());
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }));
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    std::vector<std::thread> counterThreads;
    for (int i = 0; i < 4; ++i) {
        counterThreads.emplace_back(counterWorker);
    }
    for (auto& t : counterThreads) t.join();

    logger.writeLine("--- Р РµР·СѓР»СЊС‚Р°С‚С‹ СЃС‡РµС‚С‡РёРєРѕРІ ---");
    logger.writeLine("РћР±С‹С‡РЅС‹Р№ int (Data Race): " + std::to_string(normalCounter));
    logger.writeLine("std::atomic<int>: " + std::to_string(atomicCounter));

    ThreadArgs asyncArgs{88, "AsyncFuture"};
    std::future<std::string> fut = std::async(std::launch::async, valueWorker, std::ref(asyncArgs));
    logger.writeLine("--- Р”Р°РЅРЅС‹Рµ РёР· future ---");
    logger.writeLine(fut.get());

    logger.writeLine("--- РЎС‚Р°СЂС‚ Producer-Consumer ---");
    std::thread tProd(producer);
    std::thread tCons(consumer);
    tProd.join();
    tCons.join();

    logger.writeLine("main: СЂР°Р±РѕС‚Р° СѓСЃРїРµС€РЅРѕ Р·Р°РІРµСЂС€РµРЅР°.");
    return 0;
}
