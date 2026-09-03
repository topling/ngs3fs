#define _FILE_OFFSET_BITS 64

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int parse_size(const char* text, size_t* value) {
  char* end = NULL;
  errno = 0;
  const unsigned long long parsed = strtoull(text, &end, 0);
  if (errno != 0 || end == text || *end != '\0' || parsed > SIZE_MAX) {
    return 0;
  }
  *value = (size_t)parsed;
  return 1;
}

static int write_all(int fd, const void* data, size_t size) {
  const unsigned char* bytes = data;
  size_t offset = 0;
  while (offset != size) {
    const ssize_t result = write(fd, bytes + offset, size - offset);
    if (result > 0) {
      offset += (size_t)result;
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      return 0;
    }
  }
  return 1;
}

int main(int argc, char** argv) {
  if (argc != 5) {
    fprintf(stderr, "usage: %s FILE BYTES BLOCK_SIZE READY_FILE\n", argv[0]);
    return 2;
  }
  size_t size;
  size_t block_size;
  if (!parse_size(argv[2], &size) || !parse_size(argv[3], &block_size) ||
      size == 0 || block_size == 0) {
    fprintf(stderr, "invalid cache recovery fixture size\n");
    return 2;
  }
  unsigned char* block = calloc(1, block_size);
  if (block == NULL) {
    fprintf(stderr, "cache recovery fixture allocation failed\n");
    return 1;
  }
  const int file = open(argv[1], O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                        0644);
  if (file < 0) {
    fprintf(stderr, "open failed: %s\n", strerror(errno));
    free(block);
    return 1;
  }
  size_t offset = 0;
  while (offset != size) {
    const size_t count = size - offset < block_size
        ? size - offset : block_size;
    if (!write_all(file, block, count)) {
      fprintf(stderr, "write failed: %s\n", strerror(errno));
      close(file);
      free(block);
      return 1;
    }
    offset += count;
  }
  free(block);

  const int ready = open(argv[4], O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
                         0600);
  if (ready < 0 || !write_all(ready, "ready\n", 6)) {
    fprintf(stderr, "ready marker failed: %s\n", strerror(errno));
    close(ready);
    close(file);
    return 1;
  }
  close(ready);
  for (;;) {
    pause();
  }
}
