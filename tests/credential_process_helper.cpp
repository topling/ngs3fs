#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

int main(int argc, char** argv) {
  if (argc == 3) {
    const int marker = ::open(argv[1], O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
    if (marker < 0) return 1;
    ::close(marker);
    const long delay = strtol(argv[2], nullptr, 10);
    timespec pause{.tv_sec = delay / 1000, .tv_nsec = (delay % 1000) * 1'000'000};
    while (::nanosleep(&pause, &pause) != 0) {}
  }
  fprintf(stdout,
          "{\"Version\":1,\"AccessKeyId\":\"process-key\","
          "\"SecretAccessKey\":\"process-secret\","
          "\"SessionToken\":\"process-token\","
          "\"Expiration\":\"2099-01-02T03:04:05Z\"}\n");
  return 0;
}
