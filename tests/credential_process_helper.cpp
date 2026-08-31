#include <stdio.h>

int main() {
  fprintf(stdout,
          "{\"Version\":1,\"AccessKeyId\":\"process-key\","
          "\"SecretAccessKey\":\"process-secret\","
          "\"SessionToken\":\"process-token\","
          "\"Expiration\":\"2099-01-02T03:04:05Z\"}\n");
  return 0;
}
